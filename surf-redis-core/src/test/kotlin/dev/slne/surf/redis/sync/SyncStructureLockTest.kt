package dev.slne.surf.redis.sync

import dev.slne.surf.redis.sync.set.SyncSetImpl
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.junit.jupiter.api.Assertions.assertTimeoutPreemptively
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.thread
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes

/** Pins the reentrancy and consistency contract of [SyncStructureLock] and the structures using it. */
class SyncStructureLockTest {
    private val timeout = Duration.ofSeconds(20)

    @Test
    fun `a map removeIf predicate can read the map it is filtering`() = assertTimeoutPreemptively(timeout) {
        MapRemovalProbe().use { probe ->
            probe.seed((0 until 10).associate { it.toString() to it.toString() })

            val removed = probe.remove(awaited = false) { key, _ ->
                probe.map.size() in 1..10 && probe.map.containsKey(key) && probe.map.get(key) == key && key % 2 == 0
            }

            assertTrue(removed)
            assertEquals(setOf(0, 2, 4, 6, 8), probe.notifications.toSet())
        }
    }

    @Test
    fun `a list removeIf predicate can read the list it is filtering`() = assertTimeoutPreemptively(timeout) {
        ListRemovalProbe().use { probe ->
            probe.seed((0 until 10).map { it.toString() })

            val removed = probe.remove(awaited = false) { value ->
                probe.list.size() in 1..10 && probe.list.contains(value) && probe.list.get(value) == value && value >= 5
            }

            assertTrue(removed)
            assertEquals(listOf(5, 6, 7, 8, 9), probe.notifications)
        }
    }

    @Test
    fun `a set removeIf predicate can read the set it is filtering`() = assertTimeoutPreemptively(timeout) {
        SyncMapEventFixture().use { fixture ->
            fixture.scriptResult = listOf(0L, 0L)
            val set = SyncSetImpl(fixture.api, "lock-set", 1.minutes, IntCodec)
            try {
                setOverride.invoke(
                    set,
                    AbstractSyncStructure.SimpleVersionedSnapshot((0 until 10).mapTo(LinkedHashSet()) { "$it" }, 1L)
                )

                assertTrue(set.removeIf { set.size() in 1..10 && set.contains(it) && it < 3 })
                assertEquals(7, set.size())
            } finally {
                set.dispose()
            }
        }
    }

    @Test
    fun `an out-of-range list read still fails with IndexOutOfBoundsException`() {
        ListRemovalProbe().use { probe ->
            probe.seed(listOf("1", "2"))
            assertFailsWith<IndexOutOfBoundsException> { probe.list.get(2) }
        }
    }

    @Test
    fun `optimistic lookups never return another key's value while the table grows and shrinks`() =
        assertTimeoutPreemptively(timeout) {
            val lock = SyncStructureLock()
            val map = Object2ObjectOpenHashMap<String, String>()
            val keys = Array(512) { "key-$it" }
            val failure = AtomicReference<Throwable?>()
            val deadline = System.nanoTime() + 1_000_000_000L

            val writer = thread(name = "lock-test-writer") {
                try {
                    while (System.nanoTime() < deadline) {
                        for (key in keys) lock.write { map[key] = key }
                        for (key in keys) lock.write { map.remove(key) }
                    }
                } catch (t: Throwable) {
                    failure.compareAndSet(null, t)
                }
            }

            var reads = 0L
            try {
                while (writer.isAlive) {
                    val key = keys[(reads++ and 511).toInt()]
                    val value = lock.optimisticRead { map[key] }
                    if (value != null) assertEquals(key, value)
                    val size = lock.optimisticRead { map.size }
                    assertTrue(size in 0..keys.size, "size=$size")
                }
            } finally {
                writer.join()
            }

            failure.get()?.let { throw it }
            assertNull(lock.optimisticRead { map[keys[0]] })
        }

    @Test
    fun `an optimistic read observes the state of exactly one completed write`() =
        assertTimeoutPreemptively(timeout) {
            val lock = SyncStructureLock()
            val map = Object2ObjectOpenHashMap<String, String>()
            lock.write { map["a"] = "0"; map["b"] = "0" }
            val failure = AtomicReference<Throwable?>()
            val deadline = System.nanoTime() + 1_000_000_000L

            val writer = thread(name = "lock-test-pair-writer") {
                try {
                    var round = 0
                    while (System.nanoTime() < deadline) {
                        val value = (++round).toString()
                        lock.write {
                            map["a"] = value
                            for (i in 0 until 64) map["churn-$i"] = value
                            for (i in 0 until 64) map.remove("churn-$i")
                            map["b"] = value
                        }
                    }
                } catch (t: Throwable) {
                    failure.compareAndSet(null, t)
                }
            }

            try {
                while (writer.isAlive) {
                    val (a, b) = lock.optimisticRead { map["a"] to map["b"] }
                    assertEquals(a, b)
                }
            } finally {
                writer.join()
            }

            failure.get()?.let { throw it }
        }

    private object IntCodec : SyncValueCodec<Int> {
        override val descriptor: String? = null
        override fun encode(value: Int) = value.toString()
        override fun decode(value: String) = value.toInt()
    }

    private companion object {
        private val setOverride = SyncSetImpl::class.java.getDeclaredMethod(
            "overrideFromRemote", AbstractSyncStructure.SimpleVersionedSnapshot::class.java
        ).apply { isAccessible = true }
    }
}
