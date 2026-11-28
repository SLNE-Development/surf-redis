package dev.slne.surf.redis.sync

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Covers stream gap recovery and conflicts around own writes.
 */
class SupersededTrackedOwnWriteTest {
    @Test
    fun `a write superseded by a snapshot installation is re-applied from the stream`() {
        SyncMapEventFixture().use { fixture ->
            fixture.seed("old")
            val write = fixture.issueOwnWrite("P", "mine", "v")
            fixture.seed("old")
            fixture.replyOwnWrite(write, 3)

            assertTrue(fixture.streamEvent("P", 2, "remote", "key\u0000next\u0000old"))
            assertTrue(fixture.streamEvent("P", 3, fixture.instanceId, "mine\u0000v"))

            assertEquals("v", fixture.map["mine"]!!.text)
            assertEquals("next", fixture.map["key"]!!.text)
            assertEquals(0, fixture.resyncs.get())
        }
    }

    @Test
    fun `a write issued after the installation is not applied twice`() {
        SyncMapEventFixture().use { fixture ->
            fixture.seed("old")
            val write = fixture.issueOwnWrite("P", "mine", "v")
            fixture.replyOwnWrite(write, 2)

            assertTrue(fixture.streamEvent("P", 2, fixture.instanceId, "mine\u0000v"))

            assertNull(fixture.map["mine"])
            assertEquals(0, fixture.resyncs.get())
        }
    }

    @Test
    fun `an own event arriving before the superseded write's reply waits for it`() {
        SyncMapEventFixture().use { fixture ->
            fixture.seed("old")
            val write = fixture.issueOwnWrite("P", "mine", "v")
            fixture.seed("old")

            assertFalse(fixture.streamEvent("P", 2, fixture.instanceId, "mine\u0000v"))
            assertNull(fixture.map["mine"])

            fixture.replyOwnWrite(write, 2)
            assertTrue(fixture.streamEvent("P", 2, fixture.instanceId, "mine\u0000v"))

            assertEquals("v", fixture.map["mine"]!!.text)
            assertEquals(0, fixture.resyncs.get())
        }
    }

    @Test
    fun `a re-applied write that conflicts with a later own write resyncs instead`() {
        SyncMapEventFixture().use { fixture ->
            fixture.seed("old")
            val write = fixture.issueOwnWrite("P", "mine", "v")
            fixture.seed("old")
            fixture.issueOwnWrite("C")
            fixture.replyOwnWrite(write, 2)

            assertTrue(fixture.streamEvent("P", 2, fixture.instanceId, "mine\u0000v"))
            fixture.awaitResync()

            assertEquals(1, fixture.resyncs.get())
            assertNull(fixture.map["mine"])
        }
    }

    @Test
    fun `a re-applied write that commutes with a later own write is applied`() {
        SyncMapEventFixture().use { fixture ->
            fixture.seed("old")
            val write = fixture.issueOwnWrite("P", "mine", "v")
            fixture.seed("old")
            fixture.issueOwnWrite("P", "other", "x")
            fixture.replyOwnWrite(write, 2)

            assertTrue(fixture.streamEvent("P", 2, fixture.instanceId, "mine\u0000v"))

            assertEquals("v", fixture.map["mine"]!!.text)
            assertEquals(0, fixture.resyncs.get())
        }
    }
}
