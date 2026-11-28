package dev.slne.surf.redis.sync

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** Pins the observable contract of [dev.slne.surf.redis.sync.map.SyncMapImpl] batch removal. */
class MapRemovalTest {
    private fun seeded(size: Int, probe: MapRemovalProbe): MapRemovalProbe {
        val encoded = LinkedHashMap<String, String>(size * 2)
        for (index in 0 until size) encoded[index.toString()] = index.toString()
        probe.seed(encoded)
        return probe
    }

    @Test
    fun `removes exactly the matching entries and reports each one once`() {
        for (awaited in listOf(false, true)) MapRemovalProbe().use { probe ->
            seeded(100, probe)

            assertTrue(probe.remove(awaited) { key, value -> key % 3 == 0 && value == key }, "awaited=$awaited")

            val expected = (0 until 100).filter { it % 3 == 0 }
            assertEquals(100 - expected.size, probe.map.size(), "awaited=$awaited")
            assertEquals(expected.toSet(), probe.notifications.toSet(), "awaited=$awaited")
            assertEquals(expected.size, probe.notifications.size, "awaited=$awaited")
            for (key in expected) assertEquals(null, probe.map.get(key), "awaited=$awaited")
        }
    }

    @Test
    fun `passes exactly the removed keys to the remove-many script`() {
        for (awaited in listOf(false, true)) MapRemovalProbe().use { probe ->
            seeded(20, probe)

            probe.remove(awaited) { key, _ -> key % 2 == 0 }

            val expected = probe.notifications.map { it.toString() }
            assertEquals((0 until 20).filter { it % 2 == 0 }.toSet(), probe.notifications.toSet(), "awaited=$awaited")
            assertEquals(expected, probe.arguments.takeLast(expected.size), "awaited=$awaited")
        }
    }

    @Test
    fun `reports the notifications in removal order`() {
        for (awaited in listOf(false, true)) MapRemovalProbe().use { probe ->
            seeded(50, probe)

            val visited = mutableListOf<Int>()
            probe.remove(awaited) { key, _ -> visited.add(key); key % 2 == 0 }

            assertEquals(visited.filter { it % 2 == 0 }, probe.notifications, "awaited=$awaited")
        }
    }

    @Test
    fun `a predicate that never matches changes nothing`() {
        for (awaited in listOf(false, true)) MapRemovalProbe().use { probe ->
            seeded(20, probe)

            assertFalse(probe.remove(awaited) { _, _ -> false }, "awaited=$awaited")
            assertEquals(20, probe.map.size(), "awaited=$awaited")
            assertTrue(probe.notifications.isEmpty(), "awaited=$awaited")
            assertTrue(probe.arguments.isEmpty(), "awaited=$awaited")
        }
    }

    @Test
    fun `a throwing predicate keeps the already removed entries local and silent`() {
        for (awaited in listOf(false, true)) MapRemovalProbe().use { probe ->
            seeded(20, probe)

            var seen = 0
            assertFailsWith<IllegalStateException>("awaited=$awaited") {
                probe.remove(awaited) { _, _ ->
                    if (seen++ == 10) error("predicate failed")
                    true
                }
            }

            assertEquals(10, probe.map.size(), "awaited=$awaited")
            assertTrue(probe.notifications.isEmpty(), "awaited=$awaited")
            assertTrue(probe.arguments.isEmpty(), "awaited=$awaited")
        }
    }
}
