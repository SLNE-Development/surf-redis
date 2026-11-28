package dev.slne.surf.redis.cache

import dev.slne.surf.redis.RedisApi
import dev.slne.surf.redis.internal.RedissonConnectionKey
import dev.slne.surf.redis.internal.RedissonConnector
import dev.slne.surf.redis.internal.SharedRedissonClientRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.json.Json
import org.redisson.api.RScriptReactive
import org.redisson.api.RedissonClient
import org.redisson.api.RedissonReactiveClient
import org.redisson.codec.BaseEventCodec
import org.redisson.config.Config
import org.redisson.misc.RedisURI
import reactor.core.publisher.Mono
import java.lang.reflect.Proxy
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * Checks that [SimpleRedisCacheImpl] keeps the Redis entry of a key alive while the key is only read from
 * the near cache, so that other instances still find it.
 */
class SimpleCacheTouchTest {
    @Test
    fun `near hits refresh the Redis TTL more often than the TTL elapses`() {
        val touches = AtomicInteger()
        val script = proxy<RScriptReactive> { name, args ->
            when (name) {
                "scriptLoad" -> {
                    val body = args!![0] as String
                    Mono.just(SCRIPTS.entries.single { it.value == body }.key)
                }

                "evalSha" -> when (args!![1]) {
                    "get_and_touch" -> Mono.just(Json.encodeToString(String.serializer(), "v"))
                    "touch_value" -> Mono.fromCallable { touches.incrementAndGet().toLong() }
                    else -> error("Unexpected script ${args[1]}")
                }

                else -> error("Unexpected script operation $name")
            }
        }
        val reactive = proxy<RedissonReactiveClient> { name, _ ->
            if (name == "getScript") script else error("Unexpected reactive operation $name")
        }
        val client = proxy<RedissonClient> { name, _ ->
            when (name) {
                "reactive" -> reactive
                "shutdown" -> null
                else -> error("Unexpected client operation $name")
            }
        }

        val config = Config().apply { useSingleServer().address = "redis://127.0.0.1:6379" }
        val key = RedissonConnectionKey.of(RedisURI("redis://127.0.0.1:6379"))
        val api = RedisApi::class.java.getDeclaredConstructor(
            Config::class.java, RedissonConnectionKey::class.java, String::class.java, Json::class.java
        ).apply { isAccessible = true }.newInstance(config, key, "simple-cache-test", Json)
        val lease = SharedRedissonClientRegistry(object : RedissonConnector {
            override fun connect(config: Config) = client
            override fun detectOsType(redisson: RedissonClient): BaseEventCodec.OSType? = null
        }).acquire(key, "simple-cache-test", config)
        RedisApi::class.java.getDeclaredField("lease").apply { isAccessible = true }.set(api, lease)

        val cache = SimpleRedisCacheImpl<String, String>("touch-test", String.serializer(), ttl = 1.seconds, api = api)
        try {
            runBlocking {
                assertEquals("v", cache.getCached("k"))
                assertEquals(0, touches.get(), "a miss already refreshes the TTL")

                val end = System.nanoTime() + 1.seconds.inWholeNanoseconds
                while (System.nanoTime() < end) {
                    assertEquals("v", cache.getCached("k"))
                    Thread.sleep(20)
                }
            }
            assertTrue(touches.get() >= 2, "expected at least 2 TTL refreshes within one TTL, got ${touches.get()}")
        } finally {
            cache.dispose()
            for (name in listOf("redisListenerScope", "syncStructureScope")) {
                (RedisApi::class.java.getDeclaredField(name).apply { isAccessible = true }.get(api) as CoroutineScope)
                    .cancel()
            }
            lease.release()
        }
    }

    private companion object {
        val SCRIPTS = listOf("touch_value", "get_and_touch").associateWith { name ->
            SimpleCacheTouchTest::class.java.getResource("/lua/cache/simple/$name.lua")!!.readText()
        }

        inline fun <reified T> proxy(crossinline call: (String, Array<out Any?>?) -> Any?): T =
            Proxy.newProxyInstance(T::class.java.classLoader, arrayOf(T::class.java)) { self, method, args ->
                when (method.name) {
                    "toString" -> "SimpleCacheTouchTest${T::class.java.simpleName}"
                    "hashCode" -> System.identityHashCode(self)
                    "equals" -> self === args?.get(0)
                    else -> call(method.name, args)
                }
            } as T
    }
}
