package dev.slne.surf.redis.sync

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * An own map write whose version skips unapplied foreign events is filled from the stream instead of a
 * snapshot, unless a foreign event ordered before it touches the same key or clears the map. The same
 * check covers foreign events that arrive while the own write is still in flight.
 */
class TrackedOwnWriteGapTest {
    @Test
    fun `foreign writes to other keys fill the gap without a snapshot`() {
        SyncMapEventFixture().use { fixture ->
            fixture.seed("old")
            fixture.replyOwnWrite(fixture.issueOwnWrite("P", "mine", "v"), 3)
            fixture.streamEvent("P", 2, "remote", "key\u0000next\u0000old")
            fixture.streamEvent("P", 3, fixture.instanceId, "mine\u0000v")
            fixture.streamEvent("R", 4, "remote", "key\u0000next")

            assertEquals(0, fixture.resyncs.get())
            assertNull(fixture.map["key"])
        }
    }

    @Test
    fun `a foreign write to the own key resyncs instead of applying`() {
        SyncMapEventFixture().use { fixture ->
            fixture.seed("old")
            fixture.replyOwnWrite(fixture.issueOwnWrite("P", "key", "mine"), 3)
            fixture.streamEvent("P", 2, "remote", "key\u0000next\u0000old")
            fixture.awaitResync()

            assertEquals(1, fixture.resyncs.get())
            assertEquals("snapshot", fixture.map["key"]!!.text)
        }
    }

    @Test
    fun `a foreign clear resyncs instead of applying`() {
        SyncMapEventFixture().use { fixture ->
            fixture.seed("old")
            fixture.replyOwnWrite(fixture.issueOwnWrite("P", "mine", "v"), 3)
            fixture.streamEvent("C", 2, "remote", "")
            fixture.awaitResync()

            assertEquals(1, fixture.resyncs.get())
            assertEquals("snapshot", fixture.map["key"]!!.text)
        }
    }

    @Test
    fun `a foreign write arriving while an own clear is in flight resyncs instead of applying`() {
        SyncMapEventFixture().use { fixture ->
            fixture.seed(null)
            fixture.issueOwnWrite("C")
            fixture.streamEvent("P", 2, "remote", "key\u0000next")
            fixture.awaitResync()

            assertEquals(1, fixture.resyncs.get())
            assertEquals("snapshot", fixture.map["key"]!!.text)
        }
    }

    @Test
    fun `a foreign write to another key applies while an own write is in flight`() {
        SyncMapEventFixture().use { fixture ->
            fixture.seed("old")
            fixture.issueOwnWrite("P", "mine", "v")
            fixture.streamEvent("P", 2, "remote", "key\u0000next\u0000old")

            assertEquals(0, fixture.resyncs.get())
            assertEquals("next", fixture.map["key"]!!.text)
        }
    }

    @Test
    fun `own writes that Redis executed in the opposite order of their local mutation resync when they conflict`() {
        SyncMapEventFixture().use { fixture ->
            fixture.seed("old")
            val put = fixture.issueOwnWrite("P", "key", "mine")
            val clear = fixture.issueOwnWrite("C")
            fixture.replyOwnWrite(clear, 2)
            fixture.replyOwnWrite(put, 3)
            fixture.awaitResync()

            assertEquals(1, fixture.resyncs.get())
        }
    }

    @Test
    fun `own writes whose replies alone arrive out of order do not resync`() {
        SyncMapEventFixture().use { fixture ->
            fixture.seed("old")
            val put = fixture.issueOwnWrite("P", "key", "mine")
            val clear = fixture.issueOwnWrite("C")
            fixture.replyOwnWrite(clear, 3)
            fixture.replyOwnWrite(put, 2)

            assertEquals(0, fixture.resyncs.get())
        }
    }

    @Test
    fun `reordered own writes to different keys do not resync`() {
        SyncMapEventFixture().use { fixture ->
            fixture.seed("old")
            val first = fixture.issueOwnWrite("P", "a", "1")
            val second = fixture.issueOwnWrite("P", "b", "2")
            fixture.replyOwnWrite(second, 2)
            fixture.replyOwnWrite(first, 3)

            assertEquals(0, fixture.resyncs.get())
        }
    }
}
