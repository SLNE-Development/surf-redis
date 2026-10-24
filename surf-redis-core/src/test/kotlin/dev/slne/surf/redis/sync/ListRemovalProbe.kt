package dev.slne.surf.redis.sync

import dev.slne.surf.redis.sync.list.SyncListChange
import dev.slne.surf.redis.sync.list.SyncListImpl
import kotlinx.coroutines.runBlocking
import kotlin.time.Duration.Companion.minutes

internal class ListRemovalProbe : AutoCloseable {
    private val fixture = SyncMapEventFixture()
    val list = SyncListImpl(fixture.api, "list-removal", 1.minutes, object : SyncValueCodec<Int> {
        override val descriptor: String? = null
        override fun encode(value: Int) = value.toString()
        override fun decode(value: String) = value.toInt()
    })
    val notifications = mutableListOf<Int>()
    var arguments: List<Any?> = emptyList()
    val writes get() = fixture.resyncs.get()

    init {
        fixture.scriptResult = listOf(0L, 0L)
        fixture.onScriptArguments = { arguments = it.toList() }
        list.addListener { if (it is SyncListChange.Removed) notifications.add(it.removed) }
    }

    fun seed(encoded: List<String>) {
        snapshotMethod.invoke(list, AbstractSyncStructure.SimpleVersionedSnapshot(encoded, 1))
        notifications.clear()
        arguments = emptyList()
        fixture.resyncs.set(0)
    }

    fun remove(awaited: Boolean, predicate: (Int) -> Boolean): Boolean =
        if (awaited) runBlocking { list.removeIfAndAwait(predicate) } else list.removeIf(predicate)

    override fun close() { list.dispose(); fixture.close() }

    companion object {
        private val snapshotMethod = SyncListImpl::class.java.getDeclaredMethod(
            "overrideFromRemote", AbstractSyncStructure.SimpleVersionedSnapshot::class.java
        ).apply { isAccessible = true }
    }
}
