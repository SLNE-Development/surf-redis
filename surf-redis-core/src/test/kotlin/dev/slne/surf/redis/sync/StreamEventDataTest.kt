package dev.slne.surf.redis.sync

import dev.slne.surf.redis.RedisApi
import dev.slne.surf.redis.util.LuaScriptRegistry
import kotlin.test.*
import kotlin.time.Duration

class StreamEventDataTest {
    @Test
    fun `optional old map value distinguishes absence from an empty encoding`() {
        assertNull(PayloadAccess.optional("key\u0000value", 2))
        assertEquals("", PayloadAccess.optional("key\u0000value\u0000", 2))
        assertEquals("old", PayloadAccess.optional("key\u0000value\u0000old", 2))
        assertNull(PayloadAccess.optional("key\u0000value", Int.MAX_VALUE))
    }

    @Test
    fun `payloads preserve leading consecutive and trailing empty parts`() {
        val raw = "\u0000middle\u0000\u0000"
        for ((index, value) in listOf("", "middle", "", "").withIndex()) {
            assertEquals(value, PayloadAccess.optional(raw, index))
            assertEquals(value, PayloadAccess.required(raw, index))
        }
        assertEquals("", PayloadAccess.optional("", 0))
        assertNull(PayloadAccess.optional("", 1))
    }

    @Test
    fun `missing required parts still fail so malformed events trigger resync`() {
        val failure = assertFailsWith<IndexOutOfBoundsException> {
            PayloadAccess.required("key\u0000value", 2)
        }
        assertEquals("Missing stream payload at index 2", failure.message)
        assertFailsWith<IllegalArgumentException> { PayloadAccess.required("", -1) }
        assertFailsWith<IllegalArgumentException> { PayloadAccess.optional("", -1) }
    }

    private abstract class PayloadAccess(api: RedisApi, registry: LuaScriptRegistry) :
        AbstractStreamSyncStructure<Unit, AbstractSyncStructure.SimpleVersionedSnapshot<Unit>>(
            api, "payload-test", Duration.ZERO, registry, "test:"
        ) {
        companion object {
            fun optional(raw: String, index: Int) = StreamEventData(1, "origin", raw).payloadOrNull(index)
            fun required(raw: String, index: Int) = StreamEventData(1, "origin", raw).payload(index)
        }
    }
}
