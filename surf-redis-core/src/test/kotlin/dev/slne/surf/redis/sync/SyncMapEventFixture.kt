package dev.slne.surf.redis.sync

import dev.slne.surf.redis.RedisApi
import dev.slne.surf.redis.internal.RedissonConnectionKey
import dev.slne.surf.redis.internal.RedissonConnector
import dev.slne.surf.redis.internal.SharedRedissonClientRegistry
import dev.slne.surf.redis.sync.map.SyncMapChange
import dev.slne.surf.redis.sync.map.SyncMapImpl
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancel
import kotlinx.serialization.json.Json
import org.redisson.api.RScriptReactive
import org.redisson.api.RTopicReactive
import org.redisson.api.RedissonClient
import org.redisson.api.RedissonReactiveClient
import org.redisson.codec.BaseEventCodec
import org.redisson.config.Config
import org.redisson.misc.RedisURI
import reactor.core.publisher.Mono
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.minutes

/** Runs the real map handlers against a controlled snapshot response without opening a connection. */
internal class SyncMapEventFixture : AutoCloseable {
    class Value(var text: String)

    var beforeEncode: (Value) -> Unit = {}
    var afterEncode: () -> Unit = {}
    var scriptResult: List<Any> = listOf("key", "snapshot", "10")
    var scriptResponse: () -> Mono<out Any> = { Mono.just(scriptResult) }
    var scriptLoads = 0
    var wakeups = 0
    var onScriptArguments: (Array<*>) -> Unit = {}
    val resyncs = AtomicInteger()
    val changes = mutableListOf<SyncMapChange<String, Value>>()
    private val config = Config().apply { useSingleServer().address = "redis://127.0.0.1:6379" }
    private val key = RedissonConnectionKey.of(RedisURI("redis://127.0.0.1:6379"))
    val api = RedisApi::class.java.getDeclaredConstructor(
        Config::class.java, RedissonConnectionKey::class.java, String::class.java, Json::class.java
    ).apply { isAccessible = true }.newInstance(config, key, "map-event-test", Json)

    private val script = proxy<RScriptReactive> { name, args ->
        when (name) {
            "scriptLoad" -> { scriptLoads++; Mono.just("snapshot-sha") }
            "evalSha" -> {
                resyncs.incrementAndGet()
                onScriptArguments(args!!.last() as Array<*>)
                scriptResponse()
            }
            else -> error("Unexpected script operation $name")
        }
    }
    private val topic = proxy<RTopicReactive> { name, _ ->
        when (name) {
            "publish" -> { wakeups++; Mono.just(0L) }
            else -> error("Unexpected topic operation $name")
        }
    }
    private val reactive = proxy<RedissonReactiveClient> { name, _ ->
        when (name) {
            "getScript" -> script
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
    private val registry = SharedRedissonClientRegistry(object : RedissonConnector {
        override fun connect(config: Config) = client
        override fun detectOsType(redisson: RedissonClient): BaseEventCodec.OSType? = null
    })
    private val lease = registry.acquire(key, "map-event-test", config)

    val map = SyncMapImpl(api, "event-test", 1.minutes, object : SyncValueCodec<String> {
        override val descriptor: String? = null
        override fun encode(value: String) = value
        override fun decode(value: String) = value
    }, object : SyncValueCodec<Value> {
        override val descriptor: String? = null
        override fun encode(value: Value): String {
            beforeEncode(value)
            val encoded = value.text
            afterEncode()
            return encoded
        }
        override fun decode(value: String) = Value(value)
    })

    init {
        RedisApi::class.java.getDeclaredField("lease").apply { isAccessible = true }.set(api, lease)
        map.addListener(changes::add)
    }

    fun seed(value: String?) {
        val snapshot = AbstractSyncStructure.SimpleVersionedSnapshot(
            if (value == null) emptyMap() else mapOf("key" to value), 1
        )
        snapshotMethod.invoke(map, snapshot)
    }

    fun event(type: String, payload: String) {
        eventMethod.invoke(map, type, eventConstructor.newInstance(2L, "remote", payload))
    }

    /** Waits until a resync requested by [event] has installed its snapshot on the stream poll scheduler. */
    fun awaitResync() {
        val inFlight = resyncInFlightField.get(map) as AtomicBoolean
        val deadline = System.nanoTime() + 5_000_000_000L
        while (inFlight.get()) {
            check(System.nanoTime() < deadline) { "resync did not finish" }
            Thread.sleep(1)
        }
    }

    val instanceId: String get() = api.clientId

    fun issueOwnWrite(type: String, vararg values: String): Any =
        trackOwnWriteMethod.call(
            map,
            type,
            values,
            map.mutateLocally {},
            false,
        )!!

    fun replyOwnWrite(write: Any, version: Long) {
        resolveTrackedWriteMethod.call(
            map,
            write,
            version,
            version,
        )
    }

    /** Runs a raw stream message through the poll handler; `false` if the poll deferred it. */
    fun streamEvent(type: String, version: Long, origin: String, payload: String): Boolean =
        processStreamEventMethod.call(map, type, "$version\u0000$origin\u0000$payload") as Boolean

    override fun close() {
        map.dispose()
        for (name in listOf("redisListenerScope", "syncStructureScope")) {
            (RedisApi::class.java.getDeclaredField(name).apply { isAccessible = true }.get(api) as CoroutineScope)
                .cancel()
        }
        lease.release()
    }

    companion object {
        private val eventType = AbstractStreamSyncStructure::class.java.declaredClasses.single {
            it.simpleName == "StreamEventData"
        }
        private val eventConstructor = eventType.getDeclaredConstructor(
            Long::class.javaPrimitiveType, String::class.java, String::class.java
        ).apply { isAccessible = true }
        private val eventMethod = SyncMapImpl::class.java.getDeclaredMethod(
            "onStreamEvent", String::class.java, eventType
        ).apply { isAccessible = true }
        private val snapshotMethod = SyncMapImpl::class.java.getDeclaredMethod(
            "overrideFromRemote", AbstractSyncStructure.SimpleVersionedSnapshot::class.java
        ).apply { isAccessible = true }

        private val trackOwnWriteMethod =
            AbstractStreamSyncStructure::class.java.declaredMethods
                .single { it.name == "trackOwnWrite" }
                .apply { isAccessible = true }

        private val resolveTrackedWriteMethod =
            AbstractStreamSyncStructure::class.java.declaredMethods
                .single { it.name == "resolveTrackedWrite" }
                .apply { isAccessible = true }
        private val processStreamEventMethod = AbstractStreamSyncStructure::class.java.getDeclaredMethod(
            "processStreamEvent", String::class.java, String::class.java
        ).apply { isAccessible = true }
        /** Invokes a reflected method and rethrows its original failure. */
        private fun Method.call(target: Any, vararg args: Any?): Any? = try {
            invoke(target, *args)
        } catch (failure: InvocationTargetException) {
            throw failure.cause ?: failure
        }

        private val resyncInFlightField = AbstractStreamSyncStructure::class.java.getDeclaredField("resyncInFlight")
            .apply { isAccessible = true }

        private inline fun <reified T> proxy(crossinline call: (String, Array<out Any?>?) -> Any?): T =
            Proxy.newProxyInstance(T::class.java.classLoader, arrayOf(T::class.java)) { self, method, args ->
                when (method.name) {
                    "toString" -> "MapEventTest${T::class.java.simpleName}"
                    "hashCode" -> System.identityHashCode(self)
                    "equals" -> self === args?.get(0)
                    else -> call(method.name, args)
                }
            } as T
    }
}
