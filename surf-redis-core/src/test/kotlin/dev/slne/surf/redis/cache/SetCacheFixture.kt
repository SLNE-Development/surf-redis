package dev.slne.surf.redis.cache

import dev.slne.surf.redis.RedisApi
import dev.slne.surf.redis.internal.RedissonConnectionKey
import dev.slne.surf.redis.internal.RedissonConnector
import dev.slne.surf.redis.internal.SharedRedissonClientRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancel
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.json.Json
import org.redisson.api.*
import org.redisson.codec.BaseEventCodec
import org.redisson.config.Config
import org.redisson.misc.RedisURI
import reactor.core.publisher.Mono
import java.lang.reflect.Proxy
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.minutes

/**
 * Runs [SimpleSetRedisCacheImpl] against an in-memory Redis that counts every command.
 *
 * Elements are strings of the form `"<id>:<color>"`, indexed by [Indexes.color]. Only the commands the
 * read paths and [SimpleSetRedisCacheImpl.removeById] issue are modelled; `init()` is never called.
 */
internal class SetCacheFixture(nearValueCacheSize: Long = 10_000) : AutoCloseable {
    object Indexes : RedisSetIndexes<String>() {
        val color by indexOne<String> { it.substringAfter(':') }
    }

    /** Delay applied to every command before it executes. */
    @Volatile
    var latency: Duration = Duration.ZERO

    val values = ConcurrentHashMap<String, String>()
    val sets = ConcurrentHashMap<String, MutableSet<String>>()
    private val commands = ConcurrentHashMap<String, AtomicInteger>()

    private val config = Config().apply { useSingleServer().address = "redis://127.0.0.1:6379" }
    private val key = RedissonConnectionKey.of(RedisURI("redis://127.0.0.1:6379"))
    private val api = RedisApi::class.java.getDeclaredConstructor(
        Config::class.java, RedissonConnectionKey::class.java, String::class.java, Json::class.java
    ).apply { isAccessible = true }.newInstance(config, key, "set-cache-test", Json)

    private val script = proxy<RScriptReactive> { name, args ->
        when (name) {
            "scriptLoad" -> {
                val body = args!![0] as String
                Mono.just(SCRIPTS.entries.single { it.value == body }.key)
            }

            "evalSha" -> command("script.${args!![1]}") {
                @Suppress("UNCHECKED_CAST")
                val argv = args[4] as Array<Any>
                when (args[1]) {
                    "touch_value" -> if (values.containsKey(valueKey(argv[2] as String))) 1L else 0L
                    "remove_by_id" -> removeById(argv[7] as String)
                    else -> error("Unexpected script ${args[1]}")
                }
            }

            else -> error("Unexpected script operation $name")
        }
    }

    private val buckets = proxy<RBucketsReactive> { name, args ->
        when (name) {
            "get" -> command("buckets.get") {
                @Suppress("UNCHECKED_CAST")
                val keys = args!![0] as Array<String>
                keys.mapNotNull { key -> values[key]?.let { key to it } }.toMap()
            }

            else -> error("Unexpected buckets operation $name")
        }
    }

    private val reactive = proxy<RedissonReactiveClient> { name, args ->
        when (name) {
            "getScript" -> script
            "getBuckets" -> buckets
            "getBucket" -> bucketProxy(args!![0] as String)
            "getSet" -> setProxy(args!![0] as String)
            else -> error("Unexpected reactive operation $name")
        }
    }

    private val client = proxy<RedissonClient> { name, _ ->
        when (name) {
            "reactive" -> reactive
            "shutdown" -> null
            else -> error("Unexpected client operation $name")
        }
    }
    private val registry = SharedRedissonClientRegistry(object : RedissonConnector {
        override fun connect(config: Config) = client
        override fun detectOsType(redisson: RedissonClient): BaseEventCodec.OSType? = null
    })
    private val lease = registry.acquire(key, "set-cache-test", config)

    init {
        RedisApi::class.java.getDeclaredField("lease").apply { isAccessible = true }.set(api, lease)
    }

    val cache = SimpleSetRedisCacheImpl(
        NAMESPACE, String.serializer(), { it.substringBefore(':') }, Indexes, 1.minutes, api,
        nearValueCacheSize = nearValueCacheSize,
    )

    fun count(command: String): Int = commands[command]?.get() ?: 0
    fun resetCounts() = commands.clear()

    /** Writes [element] as `add` would, without going through the cache. */
    fun store(element: String) {
        val id = element.substringBefore(':')
        values[valueKey(id)] = Json.encodeToString(String.serializer(), element)
        set(IDS_KEY) += id
        set(indexKey(element.substringAfter(':'))) += id
    }

    fun set(key: String): MutableSet<String> = sets.computeIfAbsent(key) { ConcurrentHashMap.newKeySet() }

    private fun removeById(id: String): List<Any> {
        val removedValue = values.remove(valueKey(id)) != null
        val removedId = set(IDS_KEY).remove(id)
        if (!removedValue && !removedId) return listOf(0L)
        val touched = sets.filter { (key, members) -> key.startsWith(INDEX_PREFIX) && members.remove(id) }
            .keys.map { it.removePrefix("$PREFIX:__idx__:").replaceFirst(':', '\u0000') }
        return listOf(1L) + touched
    }

    private fun bucketProxy(key: String) = proxy<RBucketReactive<String>> { name, _ ->
        when (name) {
            "get" -> command("bucket.get") { values[key] }
            "expire" -> command("bucket.expire") { values.containsKey(key) }
            else -> error("Unexpected bucket operation $name")
        }
    }

    private fun setProxy(key: String) = proxy<RSetReactive<String>> { name, args ->
        when (name) {
            "readAll" -> command("set.readAll") { set(key).toSet() }
            "remove" -> command("set.remove") { set(key).remove(args!![0]) }
            "expire" -> command("set.expire") { sets.containsKey(key) }
            else -> error("Unexpected set operation $name")
        }
    }

    private fun command(name: String, execute: () -> Any?): Mono<Any> {
        val mono = Mono.defer {
            commands.computeIfAbsent(name) { AtomicInteger() }.incrementAndGet()
            Mono.justOrEmpty(execute())
        }
        val delay = latency
        return if (delay.isZero) mono else mono.delaySubscription(delay)
    }

    override fun close() {
        cache.dispose()
        for (name in listOf("redisListenerScope", "syncStructureScope")) {
            (RedisApi::class.java.getDeclaredField(name).apply { isAccessible = true }.get(api) as CoroutineScope)
                .cancel()
        }
        lease.release()
    }

    companion object {
        const val NAMESPACE = "scan-test"
        private const val PREFIX = "$NAMESPACE:{$NAMESPACE}"
        const val IDS_KEY = "$PREFIX:__ids__"
        private const val INDEX_PREFIX = "$PREFIX:__idx__:color:"

        fun valueKey(id: String) = "$PREFIX:__val__:$id"
        fun indexKey(color: String) = "$INDEX_PREFIX$color"

        private val SCRIPTS = listOf("touch_value", "remove_by_id").associateWith { name ->
            SetCacheFixture::class.java.getResource("/lua/cache/simple-indexed/$name.lua")!!.readText()
        }

        private inline fun <reified T> proxy(crossinline call: (String, Array<out Any?>?) -> Any?): T =
            Proxy.newProxyInstance(T::class.java.classLoader, arrayOf(T::class.java)) { self, method, args ->
                when (method.name) {
                    "toString" -> "SetCacheTest${T::class.java.simpleName}"
                    "hashCode" -> System.identityHashCode(self)
                    "equals" -> self === args?.get(0)
                    else -> call(method.name, args)
                }
            } as T
    }
}
