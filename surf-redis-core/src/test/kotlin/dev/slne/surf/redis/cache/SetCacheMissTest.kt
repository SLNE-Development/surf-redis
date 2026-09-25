package dev.slne.surf.redis.cache

import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class SetCacheMissTest {
    @Test
    fun `value miss reads and refreshes in one command and closes the refresh gate`() = SetCacheFixture().use { f ->
        f.store("a:red")

        assertEquals("a:red", runBlocking { f.cache.getCachedById("a") })
        assertEquals(1, f.count("script.get_and_touch"))
        assertEquals(0, f.count("bucket.get") + f.count("bucket.expire") + f.count("script.touch_value"))

        f.resetCounts()
        assertEquals("a:red", runBlocking { f.cache.getCachedById("a") })
        assertEquals(0, f.count("script.touch_value"), "near hit right after a miss must not touch again")
    }

    @Test
    fun `absent value costs one command and is then served from the near cache`() = SetCacheFixture().use { f ->
        assertNull(runBlocking { f.cache.getCachedById("zz") })
        assertNull(runBlocking { f.cache.getCachedById("zz") })
        assertEquals(1, f.count("script.get_and_touch"))
        assertEquals(0, f.count("bucket.get"))
    }

    @Test
    fun `cold id and index reads load and refresh each set in one command`() = SetCacheFixture().use { f ->
        listOf("a:red", "b:red").forEach(f::store)

        val byIndex = runBlocking { f.cache.findByIndexCached(SetCacheFixture.Indexes.color, "red") }
        assertEquals(setOf("a:red", "b:red"), byIndex)
        assertEquals(1, f.count("script.read_all_and_touch"))
        assertEquals(0, f.count("set.readAll") + f.count("set.expire"))

        f.cache.clearNearCacheOnly()
        f.resetCounts()
        assertEquals(setOf("a:red", "b:red"), runBlocking { f.cache.findCached { true } })
        assertEquals(1, f.count("script.read_all_and_touch"))
        assertEquals(0, f.count("set.readAll") + f.count("set.expire"))
    }
}
