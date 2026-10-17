package dev.slne.surf.redis.sync

import kotlin.test.*

class ListRemovalTest {
    @Test
    fun `removal preserves duplicate values and predicate listener and argument order`() {
        for (awaited in listOf(false, true)) ListRemovalProbe().use { probe ->
            val input = listOf(3, 2, 3, 4, 5, 6, 3)
            probe.seed(input.map(Int::toString))
            val visited = mutableListOf<Int>()
            assertTrue(probe.remove(awaited) { visited.add(it); it % 2 != 0 })
            assertEquals(input, visited)
            assertEquals(listOf(2, 4, 6), probe.list.snapshot())
            assertEquals(listOf(3, 3, 5, 3), probe.notifications)
            assertEquals(listOf("3", "3", "5", "3"), probe.arguments.takeLast(4))
            assertEquals(1, probe.writes)
        }
    }

    @Test
    fun `empty no-match and all-match cases retain their return and notification semantics`() {
        for (awaited in listOf(false, true)) ListRemovalProbe().use { probe ->
            probe.seed(emptyList())
            assertFalse(probe.remove(awaited) { error("empty predicate") })
            assertEquals(0, probe.writes)
            probe.seed(listOf("1", "2", "3"))
            assertFalse(probe.remove(awaited) { false })
            assertEquals(listOf(1, 2, 3), probe.list.snapshot())
            assertTrue(probe.notifications.isEmpty())
            assertEquals(0, probe.writes)
            assertTrue(probe.remove(awaited) { true })
            assertTrue(probe.list.snapshot().isEmpty())
            assertEquals(listOf(1, 2, 3), probe.notifications)
        }
    }

    @Test
    fun `predicate failure preserves prior removals and the unvisited tail without publishing`() {
        for (awaited in listOf(false, true)) {
            for (failAt in 0..6) ListRemovalProbe().use { probe ->
                val input = (0..6).toList()
                probe.seed(input.map(Int::toString))
                val failure = IllegalStateException("predicate")
                val visited = mutableListOf<Int>()
                assertSame(failure, assertFailsWith<IllegalStateException> {
                    probe.remove(awaited) {
                        visited.add(it)
                        if (it == failAt) throw failure
                        it % 2 == 0
                    }
                })
                assertEquals((0..failAt).toList(), visited)
                assertEquals(input.filter { it >= failAt || it % 2 != 0 }, probe.list.snapshot())
                assertTrue(probe.notifications.isEmpty())
                assertEquals(0, probe.writes)
            }
        }
    }
}
