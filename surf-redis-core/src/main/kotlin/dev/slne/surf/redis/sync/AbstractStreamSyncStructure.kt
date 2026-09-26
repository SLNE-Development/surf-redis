package dev.slne.surf.redis.sync

import dev.slne.surf.api.core.util.logger
import dev.slne.surf.redis.RedisApi
import dev.slne.surf.redis.util.*
import kotlinx.coroutines.reactor.awaitSingle
import org.jetbrains.annotations.MustBeInvokedByOverriders
import org.redisson.api.RBucketReactive
import org.redisson.api.RScript
import org.redisson.api.RStreamReactive
import org.redisson.api.stream.StreamMessageId
import reactor.core.Disposable
import reactor.core.Disposables
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import java.util.function.ToLongFunction
import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.InvocationKind
import kotlin.contracts.contract
import kotlin.time.Duration
import kotlin.time.toJavaDuration

abstract class AbstractStreamSyncStructure<L, R : AbstractSyncStructure.VersionedSnapshot>(
    api: RedisApi,
    id: String,
    ttl: Duration,
    scriptRegistry: LuaScriptRegistry,
    structureNamespace: String,
    private val codecDescriptor: String? = null,
    /**
     * Clear event type used for key-aware conflict detection, or `null` if unsupported.
     */
    private val clearEventType: String? = null,
) : AbstractSyncStructure<L, R>(api, id, ttl) {
    companion object {
        private val log = logger()

        const val STREAM_FIELD_TYPE = "T"
        const val STREAM_FIELD_MSG = "M"

        private const val MESSAGE_DELIMITER = '\u0000'
        private const val MAX_STREAM_LENGTH = 10_000
        private val NO_ARGUMENTS = emptyArray<String>()

        private fun Any?.asLongOrZero(): Long = when (this) {
            is Number -> toLong()
            null -> 0L
            else -> toString().toLongOrNull() ?: 0L
        }
    }

    protected val instanceId: String = api.clientId

    private val versions = StreamVersionTracker()

    protected val stream: RStreamReactive<String, String> by lazy {
        api.redissonReactive.getStream(streamKey, Utf8StringCodec)
    }

    private val cursorId = AtomicReference(StreamMessageId(0, 0))
    private val resyncInFlight = AtomicBoolean(false)

    private val trackedOwnWrites = ConcurrentLinkedQueue<TrackedOwnWrite>()
    private val localWriteSequence = AtomicLong()

    @Volatile
    internal var snapshotGeneration = 0L
        private set

    @Volatile
    private var snapshotVersion = 0L

    private val resyncSubscription = Disposables.swap().also(::trackDisposable)

    protected val namespace: String = "$structureNamespace${this.id}:"
    protected val dataKey = "${namespace}snapshot"
    protected val versionKey = "${namespace}version"
    protected val streamKey: String = "${namespace}stream"
    private val codecKey: String = "${namespace}codec"

    private val codecBucket: RBucketReactive<String> by lazy {
        api.redissonReactive.getBucket(codecKey, Utf8StringCodec)
    }

    protected val scriptExecutor = LuaScriptExecutor.getInstance(api, scriptRegistry)

    // Pre-computed per-instance constants reused on every Lua script invocation. Reduces
    // per-op allocations of identical Strings / List<Any> wrappers on the hot path.
    private val msgDelimiterStr: String = MESSAGE_DELIMITER.toString()
    private val streamMaxLengthStr: String = MAX_STREAM_LENGTH.toString()
    private val scriptKeys: List<Any> = listOf(dataKey, streamKey, versionKey)

    private val wakeupBus = SyncStreamWakeupBus.getInstance(api)
    private val wakeupSink = wakeupBus.register(streamKey)

    private val keyEventBus = SyncKeyEventBus.getInstance(api)
    private val resyncOnKeyEvent = Runnable(::requestResync)

    @MustBeInvokedByOverriders
    override fun init(): Mono<Void> {
        return Mono.`when`(wakeupBus.init(), keyEventBus.init())
            .then(validateCodecConfiguration())
            .then(stream.fetchLatestStreamId())
            .doOnNext { cursorId.set(it) }
            .then(Mono.fromRunnable<Void> { keyEventBus.register(dataKey, resyncOnKeyEvent) })
            .then(super.init())
            .doOnSuccess {
                trackDisposable(startPolling())
                trackDisposable(
                    RedisExpirableUtils.refreshContinuously(
                        api.redissonReactive.keys,
                        ttl,
                        dataKey,
                        streamKey,
                        versionKey,
                        *codecDescriptor?.let { arrayOf(codecKey) }.orEmpty()
                    )
                )
            }
            .then()
    }

    /**
     * Processes a stream event.
     *
     * @return whether the stream cursor may advance past this event
     */
    private fun processStreamEvent(type: String, msg: String): Boolean {
        // Parse "version<DELIM>origin<DELIM>payload" without allocating an ArrayList per message.
        val firstDelim = msg.indexOf(MESSAGE_DELIMITER)
        if (firstDelim < 0) {
            log.atWarning()
                .log(
                    "Malformed stream message for type %s: expected at least 2 parts but got 1: %s",
                    type,
                    msg
                )
            return true
        }
        val secondDelim = msg.indexOf(MESSAGE_DELIMITER, firstDelim + 1)
        if (secondDelim < 0) {
            throw IllegalArgumentException(
                "Malformed stream message for type '$type': missing payload delimiter in message: $msg"
            )
        }

        val version = parseVersion(msg, firstDelim)

        if (version == null) {
            log.atWarning()
                .log(
                    "Malformed stream message for type %s: invalid version in message: %s",
                    type,
                    msg
                )
            return true
        }

        val hasEmptyOriginPart = (firstDelim + 1 until secondDelim).all { msg[it].isWhitespace() }
        if (hasEmptyOriginPart) {
            log.atWarning()
                .log(
                    "Malformed stream message for type %s: empty origin part in message: %s",
                    type,
                    msg
                )
            return true
        }

        val lengthBetweenDelimiters = secondDelim - firstDelim - 1
        val isOwnEvent = lengthBetweenDelimiters == instanceId.length &&
                msg.regionMatches(firstDelim + 1, instanceId, 0, instanceId.length)

        if (
            isOwnEvent &&
            version == versions.current + 1L &&
            hasUnresolvedWriteFromOlderSnapshot()
        ) {
            return false
        }

        if (applyVersion(version)) {
            if (isOwnEvent) {
                reapplyIfOverwrittenBySnapshot(type, version, msg, secondDelim + 1)
            } else {
                val origin = msg.substring(firstDelim + 1, secondDelim)
                applyForeignEvent(type, StreamEventData(version, origin, msg.substring(secondDelim + 1)))
            }
        }

        removeWritesCoveredBy(version)
        return true
    }

    private fun applyForeignEvent(type: String, data: StreamEventData) {
        if (
            supportsKeyAwareGapRecovery &&
            trackedOwnWrites.isNotEmpty() &&
            conflictsWithTrackedOwnWrites(type, data)
        ) {
            requestResync()
        } else {
            onStreamEvent(type, data)
        }
    }

    private fun removeWritesCoveredBy(version: Long) {
        if (trackedOwnWrites.isNotEmpty()) {
            trackedOwnWrites.removeIf { it.lastStreamVersion <= version }
        }
    }

    private fun conflictsWithTrackedOwnWrites(type: String, data: StreamEventData): Boolean {
        val key = if (type == clearEventType) null else data.payload(0)
        val currentSnapshotGeneration = snapshotGeneration
        val currentSnapshotVersion = snapshotVersion

        for (write in trackedOwnWrites) {
            if (
                write.lastStreamVersion <= data.version ||
                write.wasOverwrittenBySnapshot(currentSnapshotGeneration, currentSnapshotVersion)
            ) {
                continue
            }

            if (write.conflictsWith(key)) {
                return true
            }
        }

        return false
    }

    private fun hasUnresolvedWriteFromOlderSnapshot(): Boolean {
        if (trackedOwnWrites.isEmpty()) {
            return false
        }

        val currentSnapshotGeneration = snapshotGeneration
        return trackedOwnWrites.any {
            !it.isResolved && it.snapshotGeneration < currentSnapshotGeneration
        }
    }

    private fun reapplyIfOverwrittenBySnapshot(
        type: String,
        version: Long,
        msg: String,
        payloadStart: Int,
    ) {
        if (trackedOwnWrites.isEmpty()) {
            return
        }

        val write = trackedOwnWrites.find { write ->
            write.isResolved && version in write.firstStreamVersion..write.lastStreamVersion
        } ?: return

        if (
            !write.wasOverwrittenBySnapshot(
                snapshotGeneration,
                snapshotVersion,
            )
        ) {
            return
        }

        val data = StreamEventData(version, instanceId, msg.substring(payloadStart))

        if (conflictsWithTrackedOwnWrites(type, data)) {
            return requestResync()
        }

        onStreamEvent(type, data)
    }

    private fun parseVersion(message: String, end: Int): Long? {
        if (end == 0) return null
        var result = 0L
        for (i in 0 until end) {
            val digit = message[i].code - '0'.code
            if (digit !in 0..9 || result > (Long.MAX_VALUE - digit) / 10L) return null
            result = result * 10L + digit
        }
        return result
    }

    private fun validateCodecConfiguration(): Mono<Void> {
        val expected = codecDescriptor
        return codecBucket.get()
            .flatMap { actual ->
                if (expected == null) {
                    Mono.error<Void>(
                        IllegalStateException(
                            "Cannot use JSON encoding for synchronized structure '$id': " +
                                    "existing Redis data uses codec '$actual'"
                        )
                    ).thenReturn(true)
                } else {
                    validateCodecDescriptor(expected, actual).thenReturn(true)
                }
            }
            .switchIfEmpty(
                if (expected == null) {
                    Mono.just(true)
                } else {
                    api.redissonReactive.keys.countExists(dataKey)
                        .flatMap { existingDataKeys ->
                            if (existingDataKeys > 0L) {
                                Mono.error(
                                    IllegalStateException(
                                        "Cannot enable custom codec '$expected' for synchronized structure '$id': " +
                                                "existing Redis data has no codec metadata"
                                    )
                                )
                            } else {
                                registerCodecDescriptor(codecBucket, expected)
                            }
                        }
                        .thenReturn(true)
                }
            )
            .then()
    }

    private fun registerCodecDescriptor(
        bucket: RBucketReactive<String>,
        descriptor: String
    ): Mono<Void> {
        val register = if (ttl > Duration.ZERO) {
            bucket.setIfAbsent(descriptor, ttl.toJavaDuration())
        } else {
            bucket.setIfAbsent(descriptor)
        }
        return register.then(bucket.get())
            .switchIfEmpty(
                Mono.error(
                    IllegalStateException("Codec metadata disappeared while initializing '$id' ($codecKey)")
                )
            )
            .flatMap { actual -> validateCodecDescriptor(descriptor, actual) }
    }

    private fun validateCodecDescriptor(expected: String, actual: String): Mono<Void> {
        return if (actual == expected) {
            Mono.empty()
        } else {
            Mono.error(
                IllegalStateException(
                    "Codec mismatch for synchronized structure '$id': expected '$expected', found '$actual'"
                )
            )
        }
    }

    protected abstract fun onStreamEvent(type: String, data: StreamEventData)

    private val supportsKeyAwareGapRecovery = clearEventType != null

    /**
     * Applies [mutation] locally and returns the current snapshot generation.
     */
    @OptIn(ExperimentalContracts::class)
    internal inline fun mutateLocally(mutation: () -> Unit): Long {
        contract {
            callsInPlace(mutation, InvocationKind.EXACTLY_ONCE)
        }

        return lock.write {
            mutation()
            snapshotGeneration
        }
    }

    protected fun writeToRemote(
        snapshotGeneration: Long,
        script: String,
        eventType: String,
        vararg values: String
    ) {
        executeWrite(trackOwnWrite(eventType, values, snapshotGeneration), script, eventType, values)
            .subscribe({}, {})
    }

    protected fun writeToRemoteAwait(
        snapshotGeneration: Long,
        script: String,
        eventType: String,
        vararg values: String,
    ): Mono<Long> {
        return executeWrite(trackOwnWrite(eventType, values, snapshotGeneration), script, eventType, values)
    }

    private fun executeWrite(
        write: TrackedOwnWrite,
        script: String,
        eventType: String,
        values: Array<out String>,
    ): Mono<Long> {
        return executeScript<Long>(script, RScript.ReturnType.LONG, eventType, values)
            .resolveWriteOnNext(write) { it }
            .resyncOnScriptError(script)
    }

    private fun trackOwnWrite(
        eventType: String,
        values: Array<out String>,
        snapshotGeneration: Long,
        batch: Boolean = false,
    ): TrackedOwnWrite {
        val keyAware = supportsKeyAwareGapRecovery

        val write = TrackedOwnWrite(
            localSequence = if (keyAware) localWriteSequence.incrementAndGet() else 0L,
            arguments = if (keyAware) values else NO_ARGUMENTS,
            isBatch = batch,
            isKeyScoped = keyAware && eventType != clearEventType,
            snapshotGeneration = snapshotGeneration,
        )

        trackedOwnWrites.add(write)
        return write
    }

    private fun <T : Any> Mono<T>.resolveWriteOnNext(
        write: TrackedOwnWrite,
        version: ToLongFunction<T>,
    ): Mono<T> = resolveWriteOnNext(write, version, version)

    private fun <T : Any> Mono<T>.resolveWriteOnNext(
        write: TrackedOwnWrite,
        first: ToLongFunction<T>,
        last: ToLongFunction<T>,
    ): Mono<T> = doOnNext {
        resolveTrackedWrite(write, first.applyAsLong(it), last.applyAsLong(it))
    }.doFinally {
        stopTrackingUnresolved(write)
    }

    private fun resolveTrackedWrite(
        write: TrackedOwnWrite,
        firstStreamVersion: Long,
        lastStreamVersion: Long,
    ) {
        if (lastStreamVersion <= 0L) {
            trackedOwnWrites.remove(write)

            if (firstStreamVersion != 0L) {
                requestResync()
            }

            return resumeDeferredPollIfNeeded(write)
        }

        val currentSnapshotGeneration = snapshotGeneration

        write.firstStreamVersion = firstStreamVersion
        write.lastStreamVersion = lastStreamVersion

        if (
            write.wasOverwrittenBySnapshot(
                currentSnapshotGeneration,
                snapshotVersion,
            )
        ) {
            return wakeupSink.emitWakeup()
        }

        if (
            supportsKeyAwareGapRecovery &&
            hasConflictingWriteReorder(write)
        ) {
            requestResync()
        }

        val outcome = if (write.isBatch) {
            versions.applyRange(firstStreamVersion, lastStreamVersion)
        } else {
            versions.apply(lastStreamVersion)
        }

        val recoverFromStream =
            outcome == StreamVersionTracker.Outcome.RESYNC &&
                    supportsKeyAwareGapRecovery &&
                    !write.isBatch &&
                    versions.isBootstrapped

        if (recoverFromStream) {
            wakeupSink.emitWakeup()
        } else if (outcome == StreamVersionTracker.Outcome.RESYNC) {
            requestResync()
        }

        // The snapshot changed while the reply was being processed.
        if (snapshotGeneration != currentSnapshotGeneration) {
            requestResync()
        }

        if (!recoverFromStream) {
            stopTrackingIfSafe(write)
        }

        resumeDeferredPollIfNeeded(write)
    }

    /**
     * Returns whether Redis reordered this write against another conflicting local write.
     */
    private fun hasConflictingWriteReorder(resolved: TrackedOwnWrite): Boolean {
        val currentSnapshotGeneration = snapshotGeneration
        val currentSnapshotVersion = snapshotVersion

        fun isReordered(other: TrackedOwnWrite) =
            !other.wasOverwrittenBySnapshot(currentSnapshotGeneration, currentSnapshotVersion) &&
                    resolved.wasReorderedWith(other)

        resolved.resolvedConflictsWhilePending?.let { conflicts ->
            for (other in conflicts) {
                if (isReordered(other)) {
                    return true
                }
            }
        }

        for (other in trackedOwnWrites) {
            if (
                other === resolved ||
                !resolved.conflictsWith(other)
            ) {
                continue
            }

            if (!other.isResolved) {
                other.addResolvedConflict(resolved)

                if (!other.isResolved) {
                    continue
                }
            }

            if (isReordered(other)) {
                return true
            }
        }

        return false
    }

    private fun stopTrackingIfSafe(write: TrackedOwnWrite) {
        if (
            write.snapshotGeneration != TrackedOwnWrite.UNRESOLVED_VERSION &&
            !resyncInFlight.get()
        ) {
            trackedOwnWrites.remove(write)
        }
    }

    private fun stopTrackingUnresolved(write: TrackedOwnWrite) {
        if (
            !write.isResolved &&
            trackedOwnWrites.remove(write)
        ) {
            resumeDeferredPollIfNeeded(write)
        }
    }

    private fun resumeDeferredPollIfNeeded(write: TrackedOwnWrite) {
        if (write.snapshotGeneration < snapshotGeneration) {
            wakeupSink.emitWakeup()
        }
    }

    private fun <T : Any> executeScript(
        script: String,
        returnType: RScript.ReturnType,
        eventType: String,
        values: Array<out String>,
    ): Mono<T> {
        val argv = Array<Any>(6 + values.size) { index ->
            when (index) {
                0 -> instanceId
                1 -> msgDelimiterStr
                2 -> streamMaxLengthStr
                3 -> STREAM_FIELD_TYPE
                4 -> STREAM_FIELD_MSG
                5 -> eventType
                else -> values[index - 6]
            }
        }
        return scriptExecutor.executeWithValues(
            script,
            RScript.Mode.READ_WRITE,
            returnType,
            scriptKeys,
            argv,
        )
    }

    private fun <T : Any> Mono<T>.resyncOnScriptError(script: String): Mono<T> =
        doOnError { throwable ->
            log.atWarning()
                .withCause(throwable)
                .log("Error executing Lua script '$script' for '$id' ($streamKey)")

            requestResync()
        }

    /**
     * Executes a mutation script that returns `{version, payload, extra?}` and applies the appended
     * event to the local view as this node's own event.
     */
    protected suspend fun writeToRemoteWithPayloadAwait(
        script: String,
        eventType: String,
        vararg values: String,
    ): RemoteWriteResult {
        val generationAtIssue = snapshotGeneration
        val write = trackOwnWrite(eventType, values, TrackedOwnWrite.UNRESOLVED_VERSION)
        val raw = executeScript<List<Any>>(script, RScript.ReturnType.LIST, eventType, values)
            .resolveWriteOnNext(write) { it.getOrNull(0).asLongOrZero() }
            .resyncOnScriptError(script)
            .awaitSingle()

        val version = raw.getOrNull(0).asLongOrZero()
        val event = if (version > 0L) {
            StreamEventData(version, instanceId, raw.getOrNull(1)?.toString().orEmpty())
        } else {
            null
        }
        if (event != null) applyRemoteFirstOwnEvent(write, generationAtIssue, eventType, event)

        return RemoteWriteResult(event, raw.getOrNull(2)?.toString())
    }

    /**
     * Executes a mutation script that returns the new version and applies [payload] to the local
     * view as this node's own event.
     *
     * [payload] defaults to [values] joined with the message delimiter, which is the event payload
     * of every script whose event carries exactly its arguments.
     *
     * @return `true` if the script mutated Redis
     */
    protected suspend fun writeToRemoteAndApplyAwait(
        script: String,
        eventType: String,
        vararg values: String,
        payload: String = values.joinToString(msgDelimiterStr),
    ): Boolean {
        val generationAtIssue = snapshotGeneration
        val write = trackOwnWrite(eventType, values, TrackedOwnWrite.UNRESOLVED_VERSION)
        val version = executeWrite(write, script, eventType, values).awaitSingle()
        if (version <= 0L) return false

        applyRemoteFirstOwnEvent(write, generationAtIssue, eventType, StreamEventData(version, instanceId, payload))
        return true
    }

    protected fun joinPayload(vararg parts: String): String = parts.joinToString(msgDelimiterStr)

    /**
     * Applies an own event produced by a remote-first write.
     */
    private fun applyRemoteFirstOwnEvent(
        write: TrackedOwnWrite,
        generationAtIssue: Long,
        eventType: String,
        event: StreamEventData,
    ) {
        val generationBeforeApply = snapshotGeneration

        applyOwnEvent(eventType, event)

        // A snapshot changed while the write was in flight or while its event was applied.
        if (
            generationBeforeApply != generationAtIssue ||
            snapshotGeneration != generationBeforeApply
        ) {
            requestResync()
        }

        write.snapshotGeneration = generationBeforeApply
        stopTrackingIfSafe(write)
    }

    private fun applyOwnEvent(eventType: String, event: StreamEventData) {
        try {
            onStreamEvent(eventType, event)
        } catch (t: Throwable) {
            log.atWarning()
                .withCause(t)
                .log("Error applying own remote mutation '$eventType' for '$id' ($streamKey)")
            requestResync()
        }
    }

    /**
     * Reads the remote snapshot and replaces the local view when the snapshot is ahead of the
     * locally applied stream version.
     */
    protected fun pullRemoteSnapshot(): Mono<R> = loadRemoteSnapshot()
        .doOnNext { snapshot ->
            if (snapshot.version > versions.current) {
                overrideFromRemote(snapshot)
            }
        }

    protected fun writeBatchToRemote(
        snapshotGeneration: Long,
        script: String,
        eventType: String,
        vararg values: String,
    ) {
        executeBatchWrite(trackOwnWrite(eventType, values, snapshotGeneration, batch = true), script, eventType, values)
            .subscribe({}, {})
    }

    protected fun writeBatchToRemoteAwait(
        snapshotGeneration: Long,
        script: String,
        eventType: String,
        vararg values: String,
    ): Mono<VersionRange> {
        return executeBatchWrite(
            trackOwnWrite(eventType, values, snapshotGeneration, batch = true),
            script,
            eventType,
            values
        )
    }

    private fun executeBatchWrite(
        write: TrackedOwnWrite,
        script: String,
        eventType: String,
        values: Array<out String>,
    ): Mono<VersionRange> {
        return executeScript<List<Any>>(script, RScript.ReturnType.LIST, eventType, values)
            .map { raw ->
                VersionRange(
                    first = raw.getOrNull(0).asLongOrZero(),
                    last = raw.getOrNull(1).asLongOrZero(),
                )
            }
            .resolveWriteOnNext(write, VersionRange::first, VersionRange::last)
            .resyncOnScriptError(script)
    }

    protected fun readAtomicSnapshot(script: String): Mono<List<Any>> {
        return scriptExecutor.execute(
            script,
            RScript.Mode.READ_WRITE,
            RScript.ReturnType.LIST,
            listOf(dataKey, versionKey),
        )
    }

    private fun applyVersion(ver: Long): Boolean {
        return when (versions.apply(ver)) {
            StreamVersionTracker.Outcome.APPLIED -> true
            StreamVersionTracker.Outcome.SKIPPED -> false
            StreamVersionTracker.Outcome.RESYNC -> {
                requestResync()
                false
            }
        }
    }

    private fun startPolling(): Disposable = stream.pollContinuously(
        cursorId = cursorId,
        wakeups = wakeupSink.asFlux(),
        shouldPoll = { !resyncInFlight.get() },
    ) {
        onSuccess { batch ->
            for ((messageId, fields) in batch) {
                if (resyncInFlight.get()) {
                    break
                }

                val type = fields[STREAM_FIELD_TYPE] ?: continue
                val msg = fields[STREAM_FIELD_MSG] ?: continue

                try {
                    if (!processStreamEvent(type, msg)) break
                    cursorId.set(messageId)

                    if (resyncInFlight.get()) {
                        break
                    }
                } catch (t: Throwable) {
                    log.atWarning()
                        .withCause(t)
                        .log("Error handling stream event for '$id' ($streamKey)")
                    requestResync()
                }
            }
        }

        onFailure { e ->
            log.atWarning().withCause(e).log("Stream poll failed for '$id' ($streamKey)")
            requestResync()
        }
    }

    protected fun requestResync() {
        if (!resyncInFlight.compareAndSet(false, true)) return

        loadFromRemote()
            .retryWhen(
                Retry.backoff(
                    Long.MAX_VALUE,
                    java.time.Duration.ofMillis(100),
                )
                    .maxBackoff(java.time.Duration.ofSeconds(5))
            )
            .doFinally {
                resyncInFlight.set(false)

                if (!isDisposed) {
                    wakeupSink.emitWakeup()
                }
            }
            .doOnSubscribe { subscription -> resyncSubscription.update(Disposable(subscription::cancel)) }
            .subscribe()
    }

    override fun dispose0() {
        wakeupBus.unregister(streamKey, wakeupSink)
        keyEventBus.unregister(dataKey, resyncOnKeyEvent)
        super.dispose0()
    }

    /**
     * Records [raw] as the current snapshot.
     *
     * Must be called under the structure write lock.
     */
    @MustBeInvokedByOverriders
    override fun overrideFromRemote(raw: R) {
        versions.bootstrap(raw.version)

        snapshotVersion = raw.version
        snapshotGeneration++

        removeWritesCoveredBy(raw.version)
    }

    protected data class StreamEventData(
        val version: Long,
        val origin: String,
        private val encodedPayload: String
    ) {
        fun payload(index: Int): String = payloadOrNull(index)
            ?: throw IndexOutOfBoundsException("Missing stream payload at index $index")

        fun payloadOrNull(index: Int): String? {
            require(index >= 0) { "Payload index must not be negative: $index" }
            var start = 0
            repeat(index) {
                val delimiter = encodedPayload.indexOf(MESSAGE_DELIMITER, start)
                if (delimiter < 0) return null
                start = delimiter + 1
            }
            val end = encodedPayload.indexOf(MESSAGE_DELIMITER, start)
            return if (end < 0) encodedPayload.substring(start) else encodedPayload.substring(start, end)
        }

    }

    /**
     * Tracks an own write until its local and stream ordering is safe to forget.
     */
    private class TrackedOwnWrite(
        val localSequence: Long,
        val arguments: Array<out String>,
        val isBatch: Boolean,
        val isKeyScoped: Boolean,
        @Volatile var snapshotGeneration: Long,
    ) {

        @Volatile
        var firstStreamVersion: Long = UNRESOLVED_VERSION

        @Volatile
        var lastStreamVersion: Long = UNRESOLVED_VERSION


        val isResolved: Boolean get() = lastStreamVersion != UNRESOLVED_VERSION

        fun conflictsWith(key: String?): Boolean {
            return !isKeyScoped ||
                    key == null ||
                    if (isBatch) {
                        key in arguments
                    } else {
                        arguments[0] == key
                    }
        }

        fun conflictsWith(other: TrackedOwnWrite): Boolean = when {
            !isKeyScoped || !other.isKeyScoped -> true
            other.isBatch -> other.arguments.any(::conflictsWith)
            else -> conflictsWith(other.arguments[0])
        }

        fun wasOverwrittenBySnapshot(
            currentSnapshotGeneration: Long,
            currentSnapshotVersion: Long,
        ): Boolean =
            snapshotGeneration < currentSnapshotGeneration &&
                    lastStreamVersion > currentSnapshotVersion

        @Volatile
        var resolvedConflictsWhilePending: ConcurrentLinkedQueue<TrackedOwnWrite>? = null
            private set

        fun addResolvedConflict(other: TrackedOwnWrite) {
            val conflicts = resolvedConflictsWhilePending ?: synchronized(this) {
                resolvedConflictsWhilePending
                    ?: ConcurrentLinkedQueue<TrackedOwnWrite>().also {
                        resolvedConflictsWhilePending = it
                    }
            }

            conflicts.add(other)
        }

        fun wasReorderedWith(other: TrackedOwnWrite): Boolean =
            (localSequence < other.localSequence) !=
                    (lastStreamVersion < other.lastStreamVersion)

        companion object {
            const val UNRESOLVED_VERSION = Long.MAX_VALUE
        }
    }

    protected data class VersionRange(
        val first: Long,
        val last: Long,
    )

    /**
     * Result of a direct remote mutation executed through [writeToRemoteWithPayloadAwait].
     *
     * [event] is the stream event the script appended, or `null` when nothing was written.
     * [extra] is the script-specific third result that is not part of the stream payload, if any.
     */
    protected class RemoteWriteResult(
        val event: StreamEventData?,
        val extra: String?,
    )
}
