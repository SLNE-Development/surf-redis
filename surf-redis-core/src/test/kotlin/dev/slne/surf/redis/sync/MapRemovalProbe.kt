package dev.slne.surf.redis.sync

import dev.slne.surf.redis.sync.map.SyncMapChange
import dev.slne.surf.redis.sync.map.SyncMapImpl
import kotlinx.coroutines.runBlocking
import kotlin.time.Duration.Companion.minutes

/** Drives [SyncMapImpl.removeIf] / [SyncMapImpl.removeIfAndAwait] without a Redis connection. */
internal class MapRemovalProbe : AutoCloseable {
    private val fixture = SyncMapEventFixture()

    private val codec = object : SyncValueCodec<Int> {
        override val descriptor: String? = null
        override fun encode(value: Int) = value.toString()
        override fun decode(value: String) = value.toInt()
    }

    val map = SyncMapImpl(fixture.api, "map-removal", 1.minutes, codec, codec)
    val notifications = mutableListOf<Int>()
    var arguments: List<Any?> = emptyList()

    init {
        fixture.scriptResult = listOf(0L, 0L)
        fixture.onScriptArguments = { arguments = it.toList() }
        map.addListener { if (it is SyncMapChange.Removed) notifications.add(it.key) }
    }

    fun seed(encoded: Map<String, String>) {
        override.invoke(map, AbstractSyncStructure.SimpleVersionedSnapshot(encoded, 1L))
        notifications.clear()
        arguments = emptyList()
        fixture.resyncs.set(0)
    }

    fun remove(awaited: Boolean, predicate: (Int, Int) -> Boolean): Boolean =
        if (awaited) runBlocking { map.removeIfAndAwait(predicate) } else map.removeIf(predicate)

    override fun close() {
        map.dispose()
        fixture.close()
    }

    companion object {
        private val override = SyncMapImpl::class.java.getDeclaredMethod(
            "overrideFromRemote", AbstractSyncStructure.SimpleVersionedSnapshot::class.java
        ).apply { isAccessible = true }
    }
}
