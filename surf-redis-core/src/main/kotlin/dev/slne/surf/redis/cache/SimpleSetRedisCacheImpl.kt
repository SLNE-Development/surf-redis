package dev.slne.surf.redis.cache

import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.Expiry
import dev.slne.surf.redis.RedisApi
import dev.slne.surf.surfapi.core.api.util.logger
import it.unimi.dsi.fastutil.objects.ObjectArrayList
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.serialization.KSerializer
import org.redisson.api.RScript
import org.redisson.api.RStreamReactive
import org.redisson.api.options.KeysScanOptions
import org.redisson.api.stream.StreamMessageId
import org.redisson.api.stream.StreamReadArgs
import org.redisson.client.codec.StringCodec
import reactor.core.Disposable
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

/**
 * Redis-backed indexed set cache with near-cache (Caffeine) and Redis Streams for invalidation.
 *
 * Redis layout (all keys share the same Redis Cluster hash slot using [namespace] tag):
 * - ids-set:   [namespace]:__ids__ -> Set<id>
 * - value:     [namespace]:__val__:<id> -> JSON string
 * - index-set: [namespace]:__idx__:<indexName>:<indexValue> -> Set<id>
 * - meta-set:  [namespace]:__meta__:<id>:<indexName> -> Set<indexValue>
 * - stream:    [namespace]:__stream__ -> Stream for cross-node invalidation
 *
 * Improvements over previous version:
 * - Uses Redis Streams instead of Pub/Sub for better reliability
 * - Fixed thread-safety issues in subscription management
 * - Better error handling with automatic recovery
 * - Batched operations to prevent memory overflow
 * - Granular invalidation in removeByIndex
 */
class SimpleSetRedisCacheImpl<T : Any>(
    private val namespace: String,
    private val serializer: KSerializer<T>,
    private val idOf: (T) -> String,
    private val indexes: RedisSetIndexes<T> = RedisSetIndexes.empty(),
    private val ttl: Duration,
    private val api: RedisApi,
    nearValueCacheSize: Long = 10_000,
    nearIndexCacheSize: Long = 50_000,
) : SimpleSetRedisCache<T> {
    private companion object {
        private val log = logger()

        private const val MAX_CONCURRENT_REDIS_OPS = 64
        private const val BATCH_SIZE = 1000
        
        private const val STREAM_FIELD_TYPE = "T"
        private const val STREAM_FIELD_MSG = "M"
        private const val STREAM_MAX_LENGTH = 10_000
        private const val STREAM_READ_COUNT = 200
        
        private val STREAM_POLL_INTERVAL = 250.milliseconds
        
        private const val OP_ALL = "ALL"
        private const val OP_VAL = "VAL"
        private const val OP_IDS = "IDS"
        private const val OP_IDX = "IDX"

        private const val IDS_LOCAL_KEY = "__ids__"

        private val nodeId = UUID.randomUUID().toString().replace("-", "")
        
        private val streamScheduler = Schedulers.newBoundedElastic(
            8,
            Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE,
            "surf-redis-cache-stream-poll",
            60,
            true
        )
    }

    init {
        require(namespace.isNotBlank()) { "namespace must not be blank" }
        requireNoNul(namespace, "namespace")
        require(!namespace.contains('{') && !namespace.contains('}')) {
            "namespace must not contain '{' or '}' (used for Redis Cluster hash tags)"
        }
    }

    private val negativeTtl = minOf(ttl / 10, 5.seconds).coerceAtLeast(250.milliseconds)

    private val slotTag = "{$namespace}"
    private val keyPrefix = "$namespace:$slotTag"
    private val streamKey = "$keyPrefix:__stream__"

    @Volatile
    private var disposed: Boolean = false
    
    private val streamDisposable = AtomicReference<Disposable?>(null)
    private val cursorId = AtomicReference<StreamMessageId>(StreamMessageId(0, 0))

    private val stream: RStreamReactive<String, String> by lazy {
        api.redissonReactive.getStream(streamKey, StringCodec.INSTANCE)
    }

    private val nearValues = buildNearCache<T>(nearValueCacheSize)
    private val nearIds = buildNearCache<Set<String>>(1)
    private val nearIndexIds = buildNearCache<Set<String>>(nearIndexCacheSize)

    private val refreshGate = Caffeine.newBuilder()
        .expireAfterWrite((ttl / 4).coerceIn(250.milliseconds, 1.hours))
        .maximumSize(100_000)
        .build<String, Unit>()

    private fun <V : Any> buildNearCache(maxSize: Long) = Caffeine.newBuilder()
        .maximumSize(maxSize)
        .expireAfter(object : Expiry<String, CacheEntry<V>> {
            private fun nanos(d: Duration): Long = d.inWholeNanoseconds.coerceAtLeast(1)

            override fun expireAfterCreate(
                key: String,
                value: CacheEntry<V>,
                currentTime: Long
            ): Long = when (value) {
                is CacheEntry.Value -> nanos(ttl)
                CacheEntry.Null -> nanos(negativeTtl)
            }

            override fun expireAfterUpdate(
                key: String,
                value: CacheEntry<V>,
                currentTime: Long,
                currentDuration: Long
            ): Long = expireAfterCreate(key, value, currentTime)

            override fun expireAfterRead(
                key: String,
                value: CacheEntry<V>,
                currentTime: Long,
                currentDuration: Long
            ): Long = when (value) {
                is CacheEntry.Value -> nanos(ttl)
                CacheEntry.Null -> currentDuration
            }
        })
        .build<String, CacheEntry<V>>()

    private fun ensureSubscribed() {
        synchronized(this) {
            if (disposed) error("Cache '$namespace' is disposed")
            if (streamDisposable.get() != null) return
            
            val disposable = startPolling()
            streamDisposable.set(disposable)
        }
    }

    override fun dispose() {
        synchronized(this) {
            if (disposed) return
            disposed = true
            
            streamDisposable.getAndSet(null)?.dispose()
        }
    }

    override fun isDisposed(): Boolean = disposed

    private fun idsRedisKey(): String = "$keyPrefix:__ids__"
    private fun valueRedisPrefix(): String = "$keyPrefix:__val__:"
    private fun valueRedisKey(id: String): String = "$keyPrefix:__val__:$id"
    private fun metaRedisKey(id: String, indexName: String): String = "$keyPrefix:__meta__:$id:$indexName"
    private fun indexRedisKey(indexName: String, indexValue: String): String =
        "$keyPrefix:__idx__:$indexName:$indexValue"

    private fun indexCacheKey(indexName: String, indexValue: String): String =
        "$indexName\u0000$indexValue"

    private fun refreshKeyVal(id: String): String = "$OP_VAL\u0000$id"
    private fun refreshKeyIds(): String = OP_IDS
    private fun refreshKeyIdx(indexName: String, indexValue: String): String =
        "$OP_IDX\u0000$indexName\u0000$indexValue"

    private suspend fun publishToStream(type: String, vararg parts: String) {
        try {
            val msg = parts.joinToString("\u0000")
            for (p in parts) require(!p.contains('\u0000')) { "Stream message part contains NUL char" }
            
            stream.add(
                StreamMessageId.AUTO_GENERATED,
                mapOf(
                    STREAM_FIELD_TYPE to type,
                    STREAM_FIELD_MSG to "$nodeId\u0000$msg"
                ),
                STREAM_MAX_LENGTH,
                true
            ).awaitSingleOrNull()
            
            // Keep stream alive
            stream.expire(ttl.toJavaDuration()).awaitSingleOrNull()
        } catch (t: Throwable) {
            log.atWarning()
                .withCause(t)
                .log("Failed to publish to cache invalidation stream $streamKey")
        }
    }

    private suspend fun publishAllInvalidation() = publishToStream(OP_ALL)
    private suspend fun publishIdsInvalidation() = publishToStream(OP_IDS)
    private suspend fun publishValueInvalidation(id: String) = publishToStream(OP_VAL, id)
    private suspend fun publishIndexInvalidation(indexName: String, indexValue: String) =
        publishToStream(OP_IDX, indexName, indexValue)

    private fun requireNoNul(s: String, label: String) {
        require(!s.contains('\u0000')) { "$label must not contain NUL char" }
    }

    private fun refreshTtl(gateKey: String, action: () -> Unit) {
        val inserted = refreshGate.asMap().putIfAbsent(gateKey, Unit) == null
        if (!inserted) return
        action()
    }

    private fun refreshValueTtl(id: String) {
        refreshTtl(refreshKeyVal(id)) {
            val argv = ObjectArrayList<Any>(4 + indexes.all.size)
            argv += keyPrefix
            argv += id
            argv += ttl.inWholeMilliseconds.toString()
            argv += indexes.all.size.toString()
            for (idx in indexes.all) argv += idx.name

            try {
                SimpleSetRedisCacheLuaScripts.execute(
                    api,
                    CacheLuaScriptRegistry.TOUCH_VALUE,
                    RScript.Mode.READ_WRITE,
                    RScript.ReturnType.LONG,
                    listOf(idsRedisKey()),
                    *argv.toTypedArray()
                )
            } catch (e: Exception) {
                log.atWarning().withCause(e).log("Failed to refresh TTL via TOUCH_VALUE for id=$id")
            }
        }
    }

    private fun refreshIdsTtl() {
        val redisKey = idsRedisKey()
        refreshTtl(refreshKeyIds()) {
            api.redissonReactive.getSet<String>(redisKey, StringCodec.INSTANCE)
                .expire(ttl.toJavaDuration())
                .doOnError { e -> log.atWarning().withCause(e).log("Failed to refresh TTL for $redisKey") }
                .subscribe()
        }
    }

    private fun refreshIndexTtl(indexName: String, indexValue: String) {
        val redisKey = indexRedisKey(indexName, indexValue)
        refreshTtl(refreshKeyIdx(indexName, indexValue)) {
            api.redissonReactive.getSet<String>(redisKey, StringCodec.INSTANCE)
                .expire(ttl.toJavaDuration())
                .doOnError { e -> log.atWarning().withCause(e).log("Failed to refresh TTL for $redisKey") }
                .subscribe()
        }
    }

    override suspend fun getCachedById(id: String): T? {
        if (disposed) throw IllegalStateException("Cache is disposed")
        ensureSubscribed()

        val normId = id.trim()
        if (normId.isEmpty()) return null
        requireNoNul(normId, "id")

        when (val entry = nearValues.getIfPresent(normId)) {
            is CacheEntry.Value -> {
                refreshValueTtl(normId)
                return entry.value
            }
            CacheEntry.Null -> {
                refreshValueTtl(normId)
                return null
            }
            null -> Unit
        }

        val bucket = api.redissonReactive.getBucket<String>(valueRedisKey(normId), StringCodec.INSTANCE)
        val raw = bucket.get().awaitSingleOrNull() ?: run {
            nearValues.put(normId, CacheEntry.Null)
            return null
        }

        bucket.expire(ttl.toJavaDuration()).awaitSingleOrNull()

        val value = api.json.decodeFromString(serializer, raw)
        nearValues.put(normId, CacheEntry.Value(value))

        refreshValueTtl(normId)

        return value
    }

    private suspend fun getAllIdsCached(): Set<String> {
        if (disposed) throw IllegalStateException("Cache is disposed")
        ensureSubscribed()

        when (val entry = nearIds.getIfPresent(IDS_LOCAL_KEY)) {
            is CacheEntry.Value -> {
                refreshIdsTtl()
                return entry.value
            }
            CacheEntry.Null -> {
                refreshIdsTtl()
                return emptySet()
            }
            null -> Unit
        }

        val redisKey = idsRedisKey()
        val idsSet = api.redissonReactive.getSet<String>(redisKey, StringCodec.INSTANCE)

        val loaded = idsSet.readAll().awaitSingleOrNull().orEmpty()
        if (loaded.isNotEmpty()) {
            idsSet.expire(ttl.toJavaDuration()).awaitSingleOrNull()
            nearIds.put(IDS_LOCAL_KEY, CacheEntry.Value(loaded))
            return loaded
        }

        val exists = api.redissonReactive.keys.countExists(redisKey).awaitSingle() > 0L
        if (exists) {
            idsSet.expire(ttl.toJavaDuration()).awaitSingleOrNull()
            nearIds.put(IDS_LOCAL_KEY, CacheEntry.Null)
            return emptySet()
        }

        // Fallback: ids key missing -> rebuild from value keys
        val prefix = valueRedisPrefix()
        val keys = api.redissonReactive.keys
            .getKeys(KeysScanOptions.defaults().pattern("$prefix*"))
            .collectList()
            .awaitSingle()

        val rebuilt = keys.asSequence()
            .filter { it.startsWith(prefix) }
            .map { it.substring(prefix.length) }
            .map { it.trim() }
            .filter { it.isNotEmpty() }
            .toSet()

        if (rebuilt.isNotEmpty()) {
            idsSet.addAll(rebuilt).awaitSingleOrNull()
            idsSet.expire(ttl.toJavaDuration()).awaitSingleOrNull()
            nearIds.put(IDS_LOCAL_KEY, CacheEntry.Value(rebuilt))
            return rebuilt
        }

        nearIds.put(IDS_LOCAL_KEY, CacheEntry.Null)
        return emptySet()
    }

    override suspend fun findCached(condition: (T) -> Boolean): Set<T> {
        if (disposed) throw IllegalStateException("Cache is disposed")
        ensureSubscribed()

        val ids = getAllIdsCached()
        if (ids.isEmpty()) return emptySet()

        val result = ConcurrentHashMap.newKeySet<T>()
        val idsRedis = api.redissonReactive.getSet<String>(idsRedisKey(), StringCodec.INSTANCE)

        // Process in batches to avoid memory overflow
        ids.chunked(BATCH_SIZE).forEach { batch ->
            coroutineScope {
                val semaphore = kotlinx.coroutines.sync.Semaphore(MAX_CONCURRENT_REDIS_OPS)
                
                batch.forEach { id ->
                    launch {
                        semaphore.withPermit {
                            try {
                                val value = getCachedById(id)
                                if (value == null) {
                                    idsRedis.remove(id).awaitSingleOrNull()
                                    nearIds.invalidate(IDS_LOCAL_KEY)
                                } else if (condition(value)) {
                                    result.add(value)
                                }
                            } catch (e: Exception) {
                                log.atWarning().withCause(e).log("Error processing cached entry with id=$id")
                            }
                        }
                    }
                }
            }
        }

        return result
    }

    override suspend fun <V : Any> findByIndexCached(index: RedisSetIndex<T, V>, value: V): Set<T> {
        if (disposed) throw IllegalStateException("Cache is disposed")
        ensureSubscribed()

        require(indexes.containsSameInstance(index)) {
            "Index '${index.name}' is not registered in this cache instance. " +
                    "Use the index object from the same RedisSetIndexes registry that you passed into the cache."
        }

        val queryValue = index.valueString(value)
        requireNoNul(index.name, "indexName")
        requireNoNul(queryValue, "indexValue")

        val cacheKey = indexCacheKey(index.name, queryValue)

        val loadedIds: Set<String> = when (val entry = nearIndexIds.getIfPresent(cacheKey)) {
            is CacheEntry.Value -> {
                refreshIndexTtl(index.name, queryValue)
                entry.value
            }
            CacheEntry.Null -> emptySet()
            null -> {
                val redisKey = indexRedisKey(index.name, queryValue)
                val set = api.redissonReactive.getSet<String>(redisKey, StringCodec.INSTANCE)
                val ids = set.readAll().awaitSingleOrNull().orEmpty()
                set.expire(ttl.toJavaDuration()).awaitSingleOrNull()

                nearIndexIds.put(cacheKey, if (ids.isEmpty()) CacheEntry.Null else CacheEntry.Value(ids))
                ids
            }
        }

        if (loadedIds.isEmpty()) return emptySet()

        val redisIndexKey = indexRedisKey(index.name, queryValue)
        val indexSet = api.redissonReactive.getSet<String>(redisIndexKey, StringCodec.INSTANCE)

        val result = ConcurrentHashMap.newKeySet<T>(loadedIds.size)
        val filteredIds = ConcurrentHashMap.newKeySet<String>(loadedIds.size)
        val changed = AtomicBoolean(false)

        // Process in batches
        loadedIds.chunked(BATCH_SIZE).forEach { batch ->
            coroutineScope {
                val semaphore = kotlinx.coroutines.sync.Semaphore(MAX_CONCURRENT_REDIS_OPS)
                
                batch.forEach { id ->
                    launch {
                        semaphore.withPermit {
                            try {
                                val element = getCachedById(id)
                                if (element == null) {
                                    indexSet.remove(id).awaitSingleOrNull()
                                    changed.set(true)
                                } else {
                                    val actual = index.extractStrings(element)
                                    if (queryValue !in actual) {
                                        indexSet.remove(id).awaitSingleOrNull()
                                        changed.set(true)
                                    } else {
                                        filteredIds.add(id)
                                        result.add(element)
                                    }
                                }
                            } catch (e: Exception) {
                                log.atWarning().withCause(e).log("Error cleaning up stale index entry for id=$id")
                                // On error, invalidate entire index to be safe
                                nearIndexIds.invalidate(cacheKey)
                            }
                        }
                    }
                }
            }
        }

        if (changed.get()) {
            nearIndexIds.put(cacheKey, if (filteredIds.isEmpty()) CacheEntry.Null else CacheEntry.Value(filteredIds))
        }

        return result
    }

    override suspend fun add(element: T): Boolean {
        if (disposed) throw IllegalStateException("Cache is disposed")
        ensureSubscribed()

        val ttlMillis = ttl.inWholeMilliseconds
        require(ttlMillis > 0) { "ttl must be > 0ms" }

        val id = idOf(element).trim()
        require(id.isNotEmpty()) { "idOf(element) returned blank id" }
        requireNoNul(id, "id")

        val raw = api.json.encodeToString(serializer, element)

        val argv = ObjectArrayList<Any>(32)
        argv += keyPrefix
        argv += id
        argv += raw
        argv += ttlMillis.toString()
        argv += indexes.all.size.toString()

        for (idx in indexes.all) {
            requireNoNul(idx.name, "indexName")

            val values = idx.extractStrings(element).toList()
            for (v in values) requireNoNul(v, "indexValue")

            argv += idx.name
            argv += values.size.toString()
            for (v in values) argv += v
        }

        @Suppress("UNCHECKED_CAST")
        val result = SimpleSetRedisCacheLuaScripts.executeSuspend<List<Any>>(
            api,
            CacheLuaScriptRegistry.UPSERT,
            RScript.Mode.READ_WRITE,
            RScript.ReturnType.LIST,
            listOf(idsRedisKey()),
            *argv.toTypedArray()
        )

        val (wasNew, touched) = parseLuaFlagAndTouched(result)

        nearValues.put(id, CacheEntry.Value(element))
        if (wasNew) nearIds.invalidate(IDS_LOCAL_KEY)
        for ((idxName, idxValue) in touched) {
            nearIndexIds.invalidate(indexCacheKey(idxName, idxValue))
        }

        publishValueInvalidation(id)
        if (wasNew) publishIdsInvalidation()
        for ((idxName, idxValue) in touched) publishIndexInvalidation(idxName, idxValue)

        return wasNew
    }

    override suspend fun findCachedOrLoad(condition: (T) -> Boolean, loader: suspend () -> T): T {
        findCached(condition).firstOrNull()?.let { return it }
        val loaded = loader()
        add(loaded)
        return loaded
    }

    override suspend fun <V : Any> findCachedByIndexOrLoad(
        index: RedisSetIndex<T, V>,
        value: V,
        loader: suspend () -> T
    ): T {
        val cached = findFirstByIndexCached(index, value)
        if (cached != null) return cached

        val loaded = loader()
        add(loaded)
        return loaded
    }

    override suspend fun findCachedOrLoadNullable(condition: (T) -> Boolean, loader: suspend () -> T?): T? {
        findCached(condition).firstOrNull()?.let { return it }
        val loaded = loader() ?: return null
        add(loaded)
        return loaded
    }

    override suspend fun <V : Any> findCachedByIndexOrLoadNullable(
        index: RedisSetIndex<T, V>,
        value: V,
        loader: suspend () -> T?
    ): T? {
        val cached = findFirstByIndexCached(index, value)
        if (cached != null) return cached

        val loaded = loader() ?: return null
        add(loaded)
        return loaded
    }

    override suspend fun removeIf(predicate: (T) -> Boolean): Boolean {
        if (disposed) throw IllegalStateException("Cache is disposed")
        ensureSubscribed()

        val ids = getAllIdsCached()
        if (ids.isEmpty()) return false

        var removedAny = false
        for (id in ids) {
            val value = getCachedById(id) ?: continue
            if (predicate(value)) {
                removedAny = true
                removeById(id)
            }
        }
        return removedAny
    }

    override suspend fun removeById(id: String): Boolean {
        if (disposed) throw IllegalStateException("Cache is disposed")
        ensureSubscribed()

        val normId = id.trim()
        if (normId.isEmpty()) return false
        requireNoNul(normId, "id")

        val argv = ObjectArrayList<Any>(8 + indexes.all.size)
        argv += keyPrefix
        argv += normId
        argv += indexes.all.size.toString()
        for (idx in indexes.all) {
            requireNoNul(idx.name, "indexName")
            argv += idx.name
        }

        @Suppress("UNCHECKED_CAST")
        val result = SimpleSetRedisCacheLuaScripts.executeSuspend<List<Any>>(
            api,
            CacheLuaScriptRegistry.REMOVE_BY_ID,
            RScript.Mode.READ_WRITE,
            RScript.ReturnType.LIST,
            listOf(idsRedisKey()),
            *argv.toTypedArray()
        )

        val (removed, touched) = parseLuaFlagAndTouched(result)

        nearValues.invalidate(normId)
        nearIds.invalidate(IDS_LOCAL_KEY)
        refreshGate.invalidate(refreshKeyVal(normId))
        for ((idxName, idxValue) in touched) {
            nearIndexIds.invalidate(indexCacheKey(idxName, idxValue))
        }

        if (removed) {
            publishValueInvalidation(normId)
            publishIdsInvalidation()
            for ((idxName, idxValue) in touched) publishIndexInvalidation(idxName, idxValue)
        }

        return removed
    }

    override suspend fun <V : Any> removeByIndex(index: RedisSetIndex<T, V>, value: V): Boolean {
        if (disposed) throw IllegalStateException("Cache is disposed")
        ensureSubscribed()

        require(indexes.containsSameInstance(index)) {
            "Index '${index.name}' is not registered in this cache instance. " +
                    "Use the index object from the same RedisSetIndexes registry that you passed into the cache."
        }

        val queryValue = index.valueString(value)
        requireNoNul(index.name, "indexName")
        requireNoNul(queryValue, "indexValue")

        val argv = ArrayList<Any>(8 + indexes.all.size)
        argv += keyPrefix
        argv += index.name
        argv += queryValue
        argv += indexes.all.size.toString()
        for (idx in indexes.all) {
            requireNoNul(idx.name, "indexName")
            argv += idx.name
        }

        @Suppress("UNCHECKED_CAST")
        val result = SimpleSetRedisCacheLuaScripts.executeSuspend<List<Any>>(
            api,
            CacheLuaScriptRegistry.REMOVE_BY_INDEX,
            RScript.Mode.READ_WRITE,
            RScript.ReturnType.LIST,
            listOf(idsRedisKey()),
            *argv.toTypedArray()
        )

        if (result.isEmpty()) return false
        
        val removedCount = when (val v = result[0]) {
            is Number -> v.toLong()
            else -> v?.toString()?.toLongOrNull() ?: 0L
        }

        if (removedCount <= 0L) return false

        // Granular invalidation using removed IDs from Lua script
        val removedIds = result.drop(1).mapNotNull { it?.toString() }.toSet()
        
        // Invalidate specific values
        for (removedId in removedIds) {
            nearValues.invalidate(removedId)
            refreshGate.invalidate(refreshKeyVal(removedId))
            publishValueInvalidation(removedId)
        }
        
        // Invalidate IDs set
        nearIds.invalidate(IDS_LOCAL_KEY)
        publishIdsInvalidation()
        
        // Invalidate the specific index that was queried
        nearIndexIds.invalidate(indexCacheKey(index.name, queryValue))
        publishIndexInvalidation(index.name, queryValue)
        
        // We can't know which other indexes were affected without loading each value,
        // so we invalidate all index caches to be safe
        nearIndexIds.invalidateAll()
        publishAllInvalidation()

        return true
    }

    override suspend fun invalidateAll(): Long {
        if (disposed) throw IllegalStateException("Cache is disposed")
        ensureSubscribed()

        val deleted = api.redissonReactive.keys.deleteByPattern("$keyPrefix:*").awaitSingle()

        clearNearCacheOnly()
        publishAllInvalidation()

        return deleted
    }

    override fun clearNearCacheOnly() {
        nearValues.invalidateAll()
        nearIds.invalidateAll()
        nearIndexIds.invalidateAll()
        refreshGate.invalidateAll()
    }

    private fun parseLuaFlagAndTouched(result: List<Any?>): Pair<Boolean, Set<Pair<String, String>>> {
        if (result.isEmpty()) return false to emptySet()

        val flag = when (val v = result[0]) {
            is Number -> v.toLong() == 1L
            else -> v?.toString()?.toLongOrNull() == 1L
        }

        val touched = LinkedHashSet<Pair<String, String>>()
        for (i in 1 until result.size) {
            val s = result[i]?.toString() ?: continue
            val parts = s.split('\u0000', limit = 2)
            if (parts.size == 2) {
                touched.add(parts[0] to parts[1])
            }
        }

        return flag to touched
    }

    private fun startPolling(): Disposable {
        return pollOnce()
            .delayElement(STREAM_POLL_INTERVAL.toJavaDuration(), streamScheduler)
            .onErrorResume { e ->
                log.atWarning().withCause(e).log("Stream poll failed for cache '$namespace' ($streamKey)")
                Mono.empty()
            }
            .repeat()
            .subscribe()
    }

    private fun pollOnce(): Mono<Void> {
        val from = cursorId.get()
        val args = StreamReadArgs.greaterThan(from).count(STREAM_READ_COUNT)

        return stream.read(args)
            .filter { it.isNotEmpty() }
            .flatMap { batch ->
                for ((messageId, fields) in batch) {
                    val type = fields[STREAM_FIELD_TYPE] ?: continue
                    val msg = fields[STREAM_FIELD_MSG] ?: continue
                    
                    try {
                        processStreamEvent(type, msg)
                        cursorId.set(messageId)
                    } catch (t: Throwable) {
                        log.atWarning().withCause(t)
                            .log("Error handling stream event for cache '$namespace' ($streamKey)")
                    }
                }
                Mono.empty()
            }
    }

    private fun processStreamEvent(type: String, msg: String) {
        val parts = msg.split('\u0000')
        if (parts.isEmpty()) return

        val origin = parts[0]
        if (origin == nodeId) return // Ignore own messages

        when (type) {
            OP_ALL -> clearNearCacheOnly()
            
            OP_VAL -> {
                val id = parts.getOrNull(1) ?: return
                nearValues.invalidate(id)
                refreshGate.invalidate(refreshKeyVal(id))
            }
            
            OP_IDS -> {
                nearIds.invalidate(IDS_LOCAL_KEY)
                refreshGate.invalidate(refreshKeyIds())
            }
            
            OP_IDX -> {
                val name = parts.getOrNull(1) ?: return
                val value = parts.getOrNull(2) ?: return
                nearIndexIds.invalidate(indexCacheKey(name, value))
                refreshGate.invalidate(refreshKeyIdx(name, value))
            }
            
            else -> {
                log.atWarning().log(
                    "Received unknown cache invalidation op '$type' on stream $streamKey"
                )
            }
        }
    }

    private sealed class CacheEntry<out V> {
        data class Value<V>(val value: V) : CacheEntry<V>()
        object Null : CacheEntry<Nothing>()
    }
}