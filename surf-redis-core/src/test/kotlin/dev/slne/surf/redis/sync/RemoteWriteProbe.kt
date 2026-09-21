package dev.slne.surf.redis.sync

import dev.slne.surf.redis.RedisApi
import dev.slne.surf.redis.util.LuaScriptRegistry
import kotlinx.coroutines.runBlocking
import reactor.core.publisher.Mono
import kotlin.time.Duration

/** Exercises the production write pipeline with a controlled script response. */
internal class RemoteWriteProbe(api: RedisApi) :
    AbstractStreamSyncStructure<Unit, AbstractSyncStructure.SimpleVersionedSnapshot<Unit>>(
        api, "write-probe", Duration.ZERO, Scripts, "test:"
    ) {
    val executor get() = scriptExecutor
    val origin get() = instanceId
    var appliedPayload: String? = null
    var snapshots = 0

    fun bootstrap() = overrideFromRemote(SimpleVersionedSnapshot(Unit, 0))

    fun write(values: Array<String>) = writeToRemoteAwait("set", "SET", *values)
    fun fire(values: Array<String>) = writeToRemote("set", "SET", *values)
    fun batch(values: Array<String>) = writeBatchToRemoteAwait("set", "SET", *values)
        .map { it.first to it.last }
    fun fireBatch(values: Array<String>) = writeBatchToRemote("set", "SET", *values)
    fun apply(values: Array<String>) = runBlocking {
        writeToRemoteAndApplyAwait("set", "SET", *values)
    }
    fun payload(values: Array<String>) = runBlocking {
        writeToRemoteWithPayloadAwait("set", "SET", *values).extra
    }

    override fun onStreamEvent(type: String, data: StreamEventData) { appliedPayload = data.payload(0) }
    override fun registerListeners0(): List<Mono<Int>> = emptyList()
    override fun unregisterListener(id: Int): Mono<*> = Mono.empty<Void>()
    override fun loadFromRemote0(): Mono<SimpleVersionedSnapshot<Unit>> {
        snapshots++
        return Mono.just(SimpleVersionedSnapshot(Unit, 100))
    }

    private object Scripts : LuaScriptRegistry("lua/sync/value") { init { load("set") } }
}
