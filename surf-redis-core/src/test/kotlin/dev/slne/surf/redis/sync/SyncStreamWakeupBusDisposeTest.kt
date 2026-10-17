package dev.slne.surf.redis.sync

import dev.slne.surf.redis.RedisApi
import dev.slne.surf.redis.internal.RedissonConnectionKey
import dev.slne.surf.redis.internal.RedissonConnector
import dev.slne.surf.redis.internal.SharedRedissonClientRegistry
import kotlinx.serialization.json.Json
import org.redisson.api.RTopicReactive
import org.redisson.api.RedissonClient
import org.redisson.api.RedissonReactiveClient
import org.redisson.codec.BaseEventCodec
import org.redisson.config.Config
import org.redisson.misc.RedisURI
import reactor.core.publisher.Mono
import java.lang.reflect.Proxy
import kotlin.test.Test
import kotlin.test.assertEquals

/** A disconnected RedisApi must drop its wakeup listener from a Redisson client other instances keep using. */
class SyncStreamWakeupBusDisposeTest {
    private val removedIds = mutableListOf<Int>()

    private val topic = proxy<RTopicReactive> { name, args ->
        when (name) {
            "addListener" -> Mono.just(LISTENER_ID)
            "removeListener" -> {
                @Suppress("UNCHECKED_CAST")
                removedIds += (args!!.single() as Array<Int>)
                Mono.empty<Void>()
            }
            else -> error("Unexpected topic operation $name")
        }
    }
    private val reactive = proxy<RedissonReactiveClient> { name, _ ->
        when (name) {
            "getTopic" -> topic
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

    @Test
    fun `disconnect removes the wakeup listener exactly once`() {
        val config = Config().apply { useSingleServer().address = "redis://127.0.0.1:6379" }
        val key = RedissonConnectionKey.of(RedisURI("redis://127.0.0.1:6379"))
        val api = RedisApi::class.java.getDeclaredConstructor(
            Config::class.java, RedissonConnectionKey::class.java, String::class.java, Json::class.java
        ).apply { isAccessible = true }.newInstance(config, key, "wakeup-dispose-test", Json)
        val registry = SharedRedissonClientRegistry(object : RedissonConnector {
            override fun connect(config: Config) = client
            override fun detectOsType(redisson: RedissonClient): BaseEventCodec.OSType? = null
        })
        val lease = registry.acquire(key, "wakeup-dispose-test", config)
        RedisApi::class.java.getDeclaredField("lease").apply { isAccessible = true }.set(api, lease)

        val bus = SyncStreamWakeupBus.getInstance(api)
        bus.init().block()

        disposeManagedResources.invoke(api)
        assertEquals(listOf(LISTENER_ID), removedIds)

        bus.dispose()
        assertEquals(listOf(LISTENER_ID), removedIds)

        lease.release()
    }

    private companion object {
        const val LISTENER_ID = 42

        val disposeManagedResources = RedisApi::class.java.getDeclaredMethod("disposeManagedResources")
            .apply { isAccessible = true }

        inline fun <reified T> proxy(crossinline call: (String, Array<out Any?>?) -> Any?): T =
            Proxy.newProxyInstance(T::class.java.classLoader, arrayOf(T::class.java)) { self, method, args ->
                when (method.name) {
                    "toString" -> "WakeupDisposeTest${T::class.java.simpleName}"
                    "hashCode" -> System.identityHashCode(self)
                    "equals" -> self === args?.get(0)
                    else -> call(method.name, args)
                }
            } as T
    }
}
