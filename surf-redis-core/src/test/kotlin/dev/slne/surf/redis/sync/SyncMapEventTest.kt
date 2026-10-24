package dev.slne.surf.redis.sync

import dev.slne.surf.redis.sync.map.SyncMapChange
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.*

class SyncMapEventTest {
    @Test
    fun `readers can access the map while the event codec is running`() {
        for (type in listOf("P", "R")) {
            SyncMapEventFixture().use { fixture ->
                Executors.newSingleThreadExecutor().use { reader ->
                    fixture.seed("old")
                    fixture.beforeEncode = {
                        assertEquals("old", reader.submit<String> { fixture.map["key"]!!.text }
                            .get(5, TimeUnit.SECONDS))
                    }
                    fixture.event(type, if (type == "P") "key\u0000next\u0000old" else "key\u0000old")
                    assertEquals(0, fixture.resyncs.get())
                }
            }
        }
    }

    @Test
    fun `intervening replacement is rechecked and accepted when its encoding matches`() {
        for (type in listOf("P", "R")) {
            SyncMapEventFixture().use { fixture ->
                fixture.seed("stale")
                fixture.afterEncode = {
                    fixture.afterEncode = {}
                    fixture.seed("expected")
                }
                fixture.event(type, if (type == "P") "key\u0000next\u0000expected" else "key\u0000expected")
                assertEquals(0, fixture.resyncs.get())
                assertEquals(if (type == "P") "next" else null, fixture.map["key"]?.text)
                assertEquals(1, fixture.changes.size)
            }
        }
    }

    @Test
    fun `same instance mutation invalidates a previously matching encoding`() {
        for (type in listOf("P", "R")) {
            SyncMapEventFixture().use { fixture ->
                fixture.seed("old")
                val original = fixture.map["key"]
                fixture.afterEncode = {
                    fixture.afterEncode = {}
                    fixture.map.removeIf { _, value ->
                        value.text = "changed"
                        false
                    }
                    assertSame(original, fixture.map["key"])
                }
                fixture.event(type, if (type == "P") "key\u0000next\u0000old" else "key\u0000old")
                fixture.awaitResync()
                assertEquals(1, fixture.resyncs.get())
                assertEquals("snapshot", fixture.map["key"]!!.text)
                assertTrue(fixture.changes.isEmpty())
            }
        }
    }

    @Test
    fun `inbound updates and removals preserve encoded equality and fresh listener values`() {
        SyncMapEventFixture().use { fixture ->
            fixture.seed("old")
            val previous = fixture.map["key"]!!
            fixture.event("P", "key\u0000next\u0000old")
            assertEquals("next", fixture.map["key"]!!.text)
            val put = assertIs<SyncMapChange.Put<String, SyncMapEventFixture.Value>>(fixture.changes.single())
            assertEquals("old", put.old!!.text)
            assertNotSame(previous, put.old)
            fixture.changes.clear()
            fixture.event("R", "key\u0000next")
            assertNull(fixture.map["key"])
            assertIs<SyncMapChange.Removed<String, SyncMapEventFixture.Value>>(fixture.changes.single())
            assertEquals(0, fixture.resyncs.get())
        }
    }

    @Test
    fun `mismatched encodings resync without publishing the rejected change`() {
        for (type in listOf("P", "R")) {
            SyncMapEventFixture().use { fixture ->
                fixture.seed("local")
                fixture.event(type, if (type == "P") "key\u0000next\u0000remote" else "key\u0000remote")
                fixture.awaitResync()
                assertEquals(1, fixture.resyncs.get())
                assertEquals("snapshot", fixture.map["key"]!!.text)
                assertTrue(fixture.changes.isEmpty())
            }
        }
    }

    @Test
    fun `inserts accept absent values and resync on an occupied key`() {
        SyncMapEventFixture().use { fixture ->
            fixture.seed(null)
            fixture.event("P", "key\u0000")
            assertEquals("", fixture.map["key"]!!.text)
            fixture.changes.clear()
            fixture.event("P", "key\u0000next")
            assertEquals(1, fixture.resyncs.get())
            assertTrue(fixture.changes.isEmpty())
        }
    }
}
