package dev.slne.surf.redis.sync

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotSame

/** Pins the contract of the local `snapshot()` readers: an independent copy with the live contents. */
class LocalSnapshotTest {
    private val kinds = listOf("map", "set", "list")

    @Test
    fun `snapshots carry the live contents`() {
        for (kind in kinds) for (keys in listOf("string", "uuid")) {
            LocalSnapshotProbe(kind, keys).use { probe ->
                probe.seed(64)
                val expected = probe.liveElements()
                assertEquals(64, expected.size, "$kind/$keys")
                assertEquals(expected.toSet(), probe.snapshotElements().toSet(), "$kind/$keys")
            }
        }
    }

    @Test
    fun `snapshots are independent copies`() {
        for (kind in kinds) LocalSnapshotProbe(kind, "string").use { probe ->
            probe.seed(64)
            val first = probe.snapshotElements()
            assertNotSame(first, probe.snapshotElements(), kind)

            first.clear()
            assertEquals(64, probe.liveSize(), kind)
            assertEquals(64, probe.snapshotElements().size, kind)

            val second = probe.snapshotElements()
            second.remove(probe.liveElements().first())
            assertEquals(63, second.size, kind)
            assertEquals(64, probe.liveSize(), kind)
        }
    }

    @Test
    fun `snapshots of an over-sized table only carry the remaining entries`() {
        for (kind in kinds) LocalSnapshotProbe(kind, "string").use { probe ->
            probe.seed(10_000)
            probe.shrink(10)
            assertEquals(10, probe.liveSize(), kind)

            val snapshot = probe.snapshotElements()
            assertEquals(10, snapshot.size, kind)
            assertEquals(probe.liveElements().toSet(), snapshot.toSet(), kind)
        }
    }
}
