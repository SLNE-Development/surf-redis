package dev.slne.surf.redis.sync

import com.github.benmanes.caffeine.cache.Caffeine
import dev.slne.surf.redis.RedisApi
import org.redisson.client.codec.StringCodec
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

internal class SyncStreamWakeupBus private constructor(
    private val api: RedisApi,
) {
    companion object {
        /**
         * Channel the sync and cache mutation scripts publish `streamKey\u0000originId` to after each XADD
         */
        internal const val CHANNEL = "surf-redis:sync:wakeup"
        private const val MESSAGE_DELIMITER = '\u0000'

        private val byApi = Caffeine.newBuilder()
            .weakKeys()
            .weakValues()
            .build<RedisApi, SyncStreamWakeupBus>()

        fun getInstance(api: RedisApi): SyncStreamWakeupBus =
            byApi.get(api) { SyncStreamWakeupBus(it) }
    }

    private val topic by lazy {
        api.redissonReactive.getTopic(CHANNEL, StringCodec.INSTANCE)
    }

    private val subscribers = ConcurrentHashMap<String, CopyOnWriteArrayList<Sinks.Many<Unit>>>()

    private val initialization: Mono<Void> by lazy(LazyThreadSafetyMode.SYNCHRONIZED) {
        topic.addListener(String::class.java) { _, message ->
            handleWakeup(message)
        }
            .then()
            .cache()
    }

    fun init(): Mono<Void> = initialization

    fun register(streamKey: String): Sinks.Many<Unit> {
        val sink = Sinks.many()
            .multicast()
            .directBestEffort<Unit>()

        subscribers.computeIfAbsent(streamKey) {
            CopyOnWriteArrayList()
        }.add(sink)

        return sink
    }

    fun unregister(
        streamKey: String,
        sink: Sinks.Many<Unit>,
    ) {
        val streamSubscribers = subscribers[streamKey] ?: return

        streamSubscribers.remove(sink)
        sink.tryEmitComplete()

        if (streamSubscribers.isEmpty()) {
            subscribers.remove(streamKey, streamSubscribers)
        }
    }

    private fun handleWakeup(message: String) {
        val delimiter = message.lastIndexOf(MESSAGE_DELIMITER)
        if (delimiter <= 0 || delimiter == message.lastIndex) {
            return
        }

        val origin = message.substring(delimiter + 1)

        // The originating RedisApi already knows about the write through the Lua result
        if (origin == api.clientId) {
            return
        }

        val streamKey = message.substring(0, delimiter)
        val streamSubscribers = subscribers[streamKey] ?: return

        for (sink in streamSubscribers) {
            sink.emitWakeup()
        }
    }
}

/** Upper bound for waiting out a concurrent emission, whose `onNext` also dispatches the stream read. */
private const val WAKEUP_COLLISION_BUDGET_NANOS = 1_000_000L

/**
 * Emits a wakeup, retrying only while another thread is inside an emission on this serialized sink.
 *
 * Wakeups are idempotent, so every other outcome (no subscriber, terminated sink, or a collision
 * outlasting [collisionBudgetNanos]) drops the signal and leaves it to the interval fallback poll.
 * Never throws for a failed emission.
 */
internal fun Sinks.Many<Unit>.emitWakeup(collisionBudgetNanos: Long = WAKEUP_COLLISION_BUDGET_NANOS) {
    if (tryEmitNext(Unit) != Sinks.EmitResult.FAIL_NON_SERIALIZED) return

    val deadline = System.nanoTime() + collisionBudgetNanos
    do {
        Thread.yield()
        if (tryEmitNext(Unit) != Sinks.EmitResult.FAIL_NON_SERIALIZED) return
    } while (System.nanoTime() - deadline < 0)
}