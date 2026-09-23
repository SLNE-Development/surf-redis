package dev.slne.surf.redis.sync

import dev.slne.surf.redis.RedisApi
import dev.slne.surf.redis.util.LuaScriptRegistry
import reactor.core.publisher.Mono
import java.lang.reflect.InvocationTargetException
import kotlin.time.Duration

/** Exercises stream routing and payload extraction without Redis polling or user codecs. */
internal class StreamParsingProbe(api: RedisApi) :
    AbstractStreamSyncStructure<Unit, AbstractSyncStructure.SimpleVersionedSnapshot<Unit>>(
        api, "parsing-probe", Duration.ZERO, object : LuaScriptRegistry("unused") {}, "test:"
    ) {
    val ownOrigin get() = instanceId
    var deliveries = 0
    var receivedOrigin = ""
    var receivedVersion = 0L
    var parts = emptyList<String?>()
    var readParts = true

    fun bootstrap(version: Long) = overrideFromRemote(SimpleVersionedSnapshot(Unit, version))

    fun process(message: String) {
        try {
            processMethod.invoke(this, "test", message)
        } catch (failure: InvocationTargetException) {
            throw failure.cause!!
        }
    }

    override fun onStreamEvent(type: String, data: StreamEventData) {
        deliveries++
        receivedOrigin = data.origin
        receivedVersion = data.version
        if (readParts) parts = listOf(data.payload(0), data.payloadOrNull(1), data.payloadOrNull(2))
    }

    override fun loadFromRemote0(): Mono<SimpleVersionedSnapshot<Unit>> = Mono.just(SimpleVersionedSnapshot(Unit, 100))

    companion object {
        private val processMethod = AbstractStreamSyncStructure::class.java.getDeclaredMethod(
            "processStreamEvent", String::class.java, String::class.java
        ).apply { isAccessible = true }

        fun readParts(raw: String, indices: List<Int>): List<String?> {
            val data = StreamEventData(1, "origin", raw)
            return indices.map(data::payloadOrNull)
        }
    }
}
