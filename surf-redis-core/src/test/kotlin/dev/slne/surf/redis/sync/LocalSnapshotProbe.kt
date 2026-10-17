package dev.slne.surf.redis.sync

import dev.slne.surf.redis.sync.list.SyncListImpl
import dev.slne.surf.redis.sync.map.SyncMapImpl
import dev.slne.surf.redis.sync.set.SyncSetImpl
import java.util.UUID
import kotlin.time.Duration.Companion.minutes

/**
 * Drives the local `snapshot()` reader of [SyncMapImpl], [SyncSetImpl] and [SyncListImpl] without a
 * Redis connection.
 *
 * [kind] selects the structure, [keys] the element type (`string` keeps the JDK's cached
 * `String.hashCode`, `uuid` recomputes on every call). [shrink] removes entries directly from the
 * backing collection so the hash table stays over-sized, which is the case where inheriting the
 * table capacity is a pessimisation. The list is the control: its snapshot already array-copies.
 */
internal class LocalSnapshotProbe(kind: String, private val keys: String) : AutoCloseable {
    private val fixture = SyncMapEventFixture()

    private val codec = object : SyncValueCodec<Any> {
        override val descriptor: String? = null
        override fun encode(value: Any) = value.toString()
        override fun decode(value: String): Any = if (keys == "uuid") UUID.fromString(value) else value
    }

    private val map =
        if (kind == "map") SyncMapImpl(fixture.api, "local-snapshot", 1.minutes, codec, codec) else null
    private val set =
        if (kind == "set") SyncSetImpl(fixture.api, "local-snapshot", 1.minutes, codec) else null
    private val list =
        if (kind == "list") SyncListImpl(fixture.api, "local-snapshot", 1.minutes, codec) else null

    fun seed(size: Int) {
        val encoded = List(size) { index ->
            if (keys == "uuid") UUID(0x1020304050607080L, index.toLong()).toString() else "element-$index"
        }
        when {
            map != null -> {
                val raw = LinkedHashMap<String, String>(size * 2)
                for (element in encoded) raw[element] = element
                mapOverride.invoke(map, AbstractSyncStructure.SimpleVersionedSnapshot(raw, 1L))
            }

            set != null ->
                setOverride.invoke(set, AbstractSyncStructure.SimpleVersionedSnapshot(LinkedHashSet(encoded), 1L))

            else ->
                listOverride.invoke(list, AbstractSyncStructure.SimpleVersionedSnapshot(encoded, 1L))
        }
    }

    /** Drops all but [keep] entries from the backing collection, bypassing the remote write path. */
    fun shrink(keep: Int) {
        val backing = when {
            map != null -> mapField.get(map)
            set != null -> setField.get(set)
            else -> listField.get(list)
        }
        val iterator = when (backing) {
            is MutableMap<*, *> -> backing.keys.iterator()
            is MutableCollection<*> -> backing.iterator()
            else -> error("Unexpected backing collection $backing")
        }
        var index = 0
        while (iterator.hasNext()) {
            iterator.next()
            if (index++ >= keep) iterator.remove()
        }
    }

    fun snapshotSize(): Int = when {
        map != null -> map.snapshot().size
        set != null -> set.snapshot().size
        else -> list!!.snapshot().size
    }

    /** A mutable view of a fresh snapshot; mutating it must not reach the live structure. */
    fun snapshotElements(): MutableCollection<Any> = when {
        map != null -> map.snapshot().keys
        set != null -> set.snapshot()
        else -> list!!.snapshot()
    }

    fun liveElements(): List<Any> = when {
        map != null -> map.snapshot().keys.toList()
        set != null -> set.snapshot().toList()
        else -> list!!.snapshot().toList()
    }

    fun liveSize(): Int = when {
        map != null -> map.size()
        set != null -> set.size()
        else -> list!!.size()
    }

    override fun close() {
        map?.dispose()
        set?.dispose()
        list?.dispose()
        fixture.close()
    }

    companion object {
        private val mapOverride = SyncMapImpl::class.java.getDeclaredMethod(
            "overrideFromRemote", AbstractSyncStructure.SimpleVersionedSnapshot::class.java
        ).apply { isAccessible = true }
        private val setOverride = SyncSetImpl::class.java.getDeclaredMethod(
            "overrideFromRemote", AbstractSyncStructure.SimpleVersionedSnapshot::class.java
        ).apply { isAccessible = true }
        private val listOverride = SyncListImpl::class.java.getDeclaredMethod(
            "overrideFromRemote", AbstractSyncStructure.SimpleVersionedSnapshot::class.java
        ).apply { isAccessible = true }
        private val mapField =
            SyncMapImpl::class.java.getDeclaredField("map").apply { isAccessible = true }
        private val setField =
            SyncSetImpl::class.java.getDeclaredField("set").apply { isAccessible = true }
        private val listField =
            SyncListImpl::class.java.getDeclaredField("list").apply { isAccessible = true }
    }
}
