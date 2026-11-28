package dev.slne.surf.redis.cache

import dev.slne.surf.redis.cache.SetCacheFixture.Companion.IDS_KEY
import dev.slne.surf.redis.cache.SetCacheFixture.Companion.indexKey
import dev.slne.surf.redis.cache.SetCacheFixture.Companion.valueKey
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SetCacheScanTest {
    @Test
    fun `index scan returns matching elements and drops stale index entries`() = SetCacheFixture().use { f ->
        listOf("a:red", "b:red", "c:blue").forEach(f::store)
        f.set(indexKey("red")) += listOf("gone", "c")

        repeat(2) {
            val found = runBlocking { f.cache.findByIndexCached(SetCacheFixture.Indexes.color, "red") }
            assertEquals(setOf("a:red", "b:red"), found)
        }
        assertEquals(setOf("a", "b"), f.set(indexKey("red")))
    }

    @Test
    fun `full scan filters by condition, drops ids without a value and serves repeats from the near cache`() =
        SetCacheFixture().use { f ->
            listOf("a:red", "b:red", "c:blue").forEach(f::store)
            f.set(IDS_KEY) += "gone"

            assertEquals(setOf("a:red", "b:red"), runBlocking { f.cache.findCached { it.endsWith("red") } })
            assertEquals(setOf("a", "b", "c"), f.set(IDS_KEY))

            f.resetCounts()
            assertEquals(setOf("c:blue"), runBlocking { f.cache.findCached { it.endsWith("blue") } })
            assertEquals(0, f.count("bucket.get") + f.count("buckets.get"))
        }

    @Test
    fun `removeIf removes exactly the matching elements`() = SetCacheFixture().use { f ->
        listOf("a:red", "b:red", "c:blue").forEach(f::store)

        assertTrue(runBlocking { f.cache.removeIf { it.endsWith("blue") } })
        assertFalse(f.values.containsKey(valueKey("c")))
        assertEquals(setOf("a", "b"), f.set(IDS_KEY))
        assertFalse(runBlocking { f.cache.removeIf { false } })
        assertEquals(setOf("a:red", "b:red"), runBlocking { f.cache.findCached { true } })
    }

    @Test
    fun `removeByIndex stops serving the removed elements and keeps the other values near-cached`() =
        SetCacheFixture().use { f ->
            listOf("a:red", "b:red", "c:blue", "d:green").forEach(f::store)
            val color = SetCacheFixture.Indexes.color
            assertEquals(4, runBlocking { f.cache.findCached { true } }.size)
            assertEquals(setOf("a:red", "b:red"), runBlocking { f.cache.findByIndexCached(color, "red") })

            assertTrue(runBlocking { f.cache.removeByIndex(color, "red") })
            assertEquals(setOf("c", "d"), f.set(IDS_KEY))
            assertFalse(f.values.containsKey(valueKey("a")))

            f.resetCounts()
            assertEquals(setOf("c:blue", "d:green"), runBlocking { f.cache.findCached { true } })
            assertEquals(emptySet(), runBlocking { f.cache.findByIndexCached(color, "red") })
            assertEquals(0, f.count("bucket.get") + f.count("buckets.get"))
            assertNull(runBlocking { f.cache.getCachedById("a") })
            assertFalse(runBlocking { f.cache.removeByIndex(color, "red") })
        }

    @Test
    fun `cold scan reads values with one bulk read per 1000 ids`() = SetCacheFixture().use { f ->
        repeat(2500) { f.store("i$it:red") }

        assertEquals(2500, runBlocking { f.cache.findCached { true } }.size)
        assertEquals(0, f.count("bucket.get"))
        assertEquals(3, f.count("buckets.get"))
    }

    @Test
    fun `scan refreshes value TTLs the refresh gate suppresses`() = SetCacheFixture(nearValueCacheSize = 1).use { f ->
        repeat(50) { f.store("i$it:red") }

        runBlocking { f.cache.findCached { true } }
        assertEquals(50, f.count("script.touch_value"))

        f.resetCounts()
        assertEquals(50, runBlocking { f.cache.findCached { true } }.size)
        assertEquals(0, f.count("script.touch_value"))
        assertTrue(f.count("bucket.expire") >= 40, "expired ${f.count("bucket.expire")} of 50 value keys")
    }
}
