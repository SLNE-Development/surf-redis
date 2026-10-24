package dev.slne.surf.redis.sync

import kotlin.test.*

class SnapshotIsolationTest {
    @Test
    fun `remote snapshots preserve element and collection isolation when installed`() {
        for (kind in listOf("map", "set", "list")) SnapshotProbe(kind, 3).use { probe ->
            probe.resetVersion()
            val remote = probe.remote()
            assertEquals(3, remote.size)
            val local = probe.local().associateBy { it.text }
            for (item in remote) assertNotSame(local[item.text], item)
            remote.first().text = "mutated"
            remote.clear()
            assertEquals(setOf("value-0", "value-1", "value-2"), probe.local().map { it.text }.toSet())
            assertEquals(0, probe.notifications)
        }
    }

    @Test
    fun `resync installs all entries and local snapshots remain structurally independent`() {
        for (kind in listOf("map", "set", "list")) SnapshotProbe(kind, 100).use { probe ->
            probe.resync()
            val first = probe.local()
            first.clear()
            assertEquals(100, probe.local().size)
            assertEquals(0, probe.notifications)
            probe.resetCounts()
            probe.remote()
            assertEquals(if (kind == "map") 200 else 100, probe.decodes)
        }
    }

    @Test
    fun `installation does not hash elements under the write lock`() {
        for (kind in listOf("map", "set", "list")) SnapshotProbe(kind, 100).use { probe ->
            probe.resync()
            assertEquals(0, probe.hashesUnderWriteLock, kind)
        }
    }

    @Test
    fun `empty snapshots remain valid`() {
        for (kind in listOf("map", "set", "list")) SnapshotProbe(kind, 0).use { probe ->
            probe.resync()
            assertTrue(probe.local().isEmpty())
            assertTrue(probe.remote().isEmpty())
        }
    }
}
