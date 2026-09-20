package dev.slne.surf.redis.sync

import dev.slne.surf.redis.sync.list.SyncListImpl
import dev.slne.surf.redis.sync.map.SyncMapImpl
import dev.slne.surf.redis.sync.set.SyncSetImpl
import kotlinx.coroutines.runBlocking
import reactor.core.publisher.Mono
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.time.Duration.Companion.minutes

/** Uses real snapshot readers and installers with an in-memory Lua response. */
internal class SnapshotProbe(val kind: String, size: Int) : AutoCloseable {
    class Item(var text: String, private val hashed: () -> Unit) {
        override fun equals(other: Any?) = other is Item && text == other.text
        override fun hashCode(): Int { hashed(); return text.hashCode() }
    }

    private val fixture = SyncMapEventFixture()
    var decodes = 0
    var hashesUnderWriteLock = 0
    var notifications = 0
    private val codec: SyncValueCodec<Item> = object : SyncValueCodec<Item> {
        override val descriptor: String? = null
        override fun encode(value: Item) = value.text
        override fun decode(value: String): Item {
            decodes++
            return Item(value) { if (lock.isWriteLockedByCurrentThread) hashesUnderWriteLock++ }
        }
    }
    val structure: AbstractStreamSyncStructure<*, *> = when (kind) {
        "map" -> SyncMapImpl(fixture.api, "snapshot-probe", 1.minutes, codec, codec)
        "set" -> SyncSetImpl(fixture.api, "snapshot-probe", 1.minutes, codec)
        "list" -> SyncListImpl(fixture.api, "snapshot-probe", 1.minutes, codec)
        else -> error(kind)
    }
    val lock: ReentrantReadWriteLock = lockField.get(structure) as ReentrantReadWriteLock
    private val versions = versionsField.get(structure) as StreamVersionTracker

    init {
        fixture.scriptResult = buildList {
            repeat(size) { index ->
                if (kind == "map") add("key-$index")
                add("value-$index")
            }
            add("2")
        }
        @Suppress("UNCHECKED_CAST")
        (structure as AbstractSyncStructure<Any, *>).addListener { notifications++ }
    }

    fun resetVersion(version: Long = 1) { versions.bootstrap(version) }
    fun resetCounts() { decodes = 0; hashesUnderWriteLock = 0 }
    fun resync() { (loadMethod.invoke(structure) as Mono<*>).block() }

    @Suppress("UNCHECKED_CAST")
    fun remote(): MutableCollection<Item> = runBlocking {
        when (kind) {
            "map" -> (structure as SyncMapImpl<Item, Item>).snapshotRemote().values
            "set" -> (structure as SyncSetImpl<Item>).snapshotRemote()
            else -> (structure as SyncListImpl<Item>).snapshotRemote()
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun local(): MutableCollection<Item> = when (kind) {
        "map" -> (structure as SyncMapImpl<Item, Item>).snapshot().values
        "set" -> (structure as SyncSetImpl<Item>).snapshot()
        else -> (structure as SyncListImpl<Item>).snapshot()
    }

    override fun close() { structure.dispose(); fixture.close() }

    companion object {
        private val lockField = AbstractSyncStructure::class.java.getDeclaredField("lock").apply { isAccessible = true }
        private val versionsField = AbstractStreamSyncStructure::class.java.getDeclaredField("versions").apply { isAccessible = true }
        private val loadMethod = AbstractSyncStructure::class.java.getDeclaredMethod("loadFromRemote").apply { isAccessible = true }
    }
}
