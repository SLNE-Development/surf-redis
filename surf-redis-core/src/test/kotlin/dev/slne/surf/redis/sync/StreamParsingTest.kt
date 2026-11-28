package dev.slne.surf.redis.sync

import kotlin.test.*

class StreamParsingTest {
    @Test
    fun `own and replayed events advance or retain version without dispatch`() {
        SyncMapEventFixture().use { fixture ->
            val probe = StreamParsingProbe(fixture.api)
            try {
                probe.bootstrap(1)
                probe.process("2\u0000${probe.ownOrigin}\u0000key\u0000value")
                probe.process("2\u0000remote\u0000replay")
                assertEquals(0, probe.deliveries)
                probe.process("3\u0000remote\u0000key\u0000value\u0000")
                assertEquals(1, probe.deliveries)
                assertEquals(3, probe.receivedVersion)
                assertEquals("remote", probe.receivedOrigin)
                assertEquals(listOf("key", "value", ""), probe.parts)
            } finally { probe.dispose() }
        }
    }

    @Test
    fun `blank unicode origins and invalid versions remain rejected`() {
        SyncMapEventFixture().use { fixture ->
            val probe = StreamParsingProbe(fixture.api)
            try {
                for (message in listOf("missing", "x\u0000remote\u0000value", "2\u0000 \t\u2003\u0000value",
                    "9223372036854775808\u0000remote\u0000value", "2\u0000\u0000value")) {
                    probe.bootstrap(1)
                    probe.process(message)
                }
                assertFailsWith<IllegalArgumentException> { probe.process("2\u0000origin") }
                assertEquals(0, probe.deliveries)
                probe.process("2\u0000${probe.ownOrigin}x\u0000value")
                assertEquals(1, probe.deliveries)
            } finally { probe.dispose() }
        }
    }

    @Test
    fun `payload indexing supports out of order reads and more than three empty parts`() {
        assertEquals(listOf("", "c", "a", "", "", null, "c"),
            StreamParsingProbe.readParts("a\u0000\u0000c\u0000\u0000", listOf(4, 2, 0, 1, 3, 5, 2)))
    }
}
