package dev.slne.surf.redis.event

import io.netty.buffer.ByteBuf
import kotlin.test.*

class EventCodecRegistryTest {
    @Test
    fun `discovers a companion codec and freezes immutable lookups`() {
        val registry = EventCodecRegistry()

        registry.registerDiscovered(CompanionEvent::class.java)
        registry.freeze()

        val registration = registry.codecForEventId(CompanionEvent::class.java.name)
        assertEquals(CompanionEvent::class.java, registration?.eventType)
        assertSame(registration, registry.codecForPublishing(CompanionEvent::class.java))
    }

    @Test
    fun `discovers a named companion codec`() {
        val registry = EventCodecRegistry()

        assertTrue(NamedCompanionEvent.Codec === registry.codecForPublishing(NamedCompanionEvent::class.java)?.codec)
    }

    @Test
    fun `discovers a codec provided by a companion property`() {
        val registry = EventCodecRegistry()

        assertTrue(ProvidedCodecEvent.codec === registry.codecForPublishing(ProvidedCodecEvent::class.java)?.codec)
    }

    @Test
    fun `nested non-companion object is not discovered`() {
        val registry = EventCodecRegistry()

        assertNull(registry.codecForPublishing(NestedObjectEvent::class.java))
    }

    @Test
    fun `inaccessible companion fails discovery`() {
        val registry = EventCodecRegistry()

        assertFailsWith<IllegalStateException> {
            registry.codecForPublishing(PrivateCompanionEvent::class.java)
        }
    }

    @Test
    fun `event without a codec remains on JSON path`() {
        val registry = EventCodecRegistry()
        registry.registerDiscovered(JsonEvent::class.java)
        registry.freeze()

        assertNull(registry.codecForPublishing(JsonEvent::class.java))
    }

    @Test
    fun `explicit registration overrides automatic discovery`() {
        val registry = EventCodecRegistry()
        registry.registerDiscovered(CompanionEvent::class.java)
        val explicit = CompanionOverrideCodec("explicit-event")

        registry.registerExplicit(CompanionEvent::class.java, explicit)
        registry.freeze()

        assertTrue(explicit === registry.codecForEventId("explicit-event")?.codec)
        assertNull(registry.codecForEventId(CompanionEvent::class.java.name))
    }

    @Test
    fun `duplicate explicit registration is rejected`() {
        val registry = EventCodecRegistry()
        registry.registerExplicit(JsonEvent::class.java, TestEventCodec("json-event"))

        assertFailsWith<IllegalArgumentException> {
            registry.registerExplicit(JsonEvent::class.java, TestEventCodec("json-event-2"))
        }
    }

    @Test
    fun `event id collision across classes is rejected`() {
        val registry = EventCodecRegistry()
        registry.registerExplicit(JsonEvent::class.java, TestEventCodec("collision"))

        assertFailsWith<IllegalArgumentException> {
            registry.registerExplicit(OtherEvent::class.java, OtherEventCodec("collision"))
        }
    }

    @Test
    fun `registration after freeze is rejected`() {
        val registry = EventCodecRegistry()
        registry.freeze()

        assertFailsWith<IllegalStateException> {
            registry.registerExplicit(JsonEvent::class.java, TestEventCodec("late"))
        }
    }

    class CompanionEvent(val value: Int) : RedisEvent() {
        companion object : RedisEventCodec<CompanionEvent> {
            override fun encode(buffer: ByteBuf, value: CompanionEvent) {
                buffer.writeInt(value.value)
            }

            override fun decode(buffer: ByteBuf) = CompanionEvent(buffer.readInt())
        }
    }

    class NamedCompanionEvent(val value: Int) : RedisEvent() {
        companion object Codec : RedisEventCodec<NamedCompanionEvent> {
            override fun encode(buffer: ByteBuf, value: NamedCompanionEvent) {
                buffer.writeInt(value.value)
            }

            override fun decode(buffer: ByteBuf) = NamedCompanionEvent(buffer.readInt())
        }
    }

    class ProvidedCodecEvent(val value: Int) : RedisEvent() {
        companion object {
            val codec: RedisEventCodec<ProvidedCodecEvent> = object : RedisEventCodec<ProvidedCodecEvent> {
                override val eventId = "provided-codec-event"

                override fun encode(buffer: ByteBuf, value: ProvidedCodecEvent) {
                    buffer.writeInt(value.value)
                }

                override fun decode(buffer: ByteBuf) = ProvidedCodecEvent(buffer.readInt())
            }
        }
    }

    class NestedObjectEvent(val value: Int) : RedisEvent() {
        object Codec : RedisEventCodec<NestedObjectEvent> {
            override fun encode(buffer: ByteBuf, value: NestedObjectEvent) {
                buffer.writeInt(value.value)
            }

            override fun decode(buffer: ByteBuf) = NestedObjectEvent(buffer.readInt())
        }
    }

    class PrivateCompanionEvent(val value: Int) : RedisEvent() {
        private companion object : RedisEventCodec<PrivateCompanionEvent> {
            override fun encode(buffer: ByteBuf, value: PrivateCompanionEvent) {
                buffer.writeInt(value.value)
            }

            override fun decode(buffer: ByteBuf) = PrivateCompanionEvent(buffer.readInt())
        }
    }

    private class JsonEvent(val value: Int) : RedisEvent()
    private class OtherEvent(val value: Int) : RedisEvent()

    private class TestEventCodec(override val eventId: String) : RedisEventCodec<JsonEvent> {
        override fun encode(buffer: ByteBuf, value: JsonEvent) = buffer.writeInt(value.value).let { Unit }
        override fun decode(buffer: ByteBuf) = JsonEvent(buffer.readInt())
    }

    private class CompanionOverrideCodec(override val eventId: String) : RedisEventCodec<CompanionEvent> {
        override fun encode(buffer: ByteBuf, value: CompanionEvent) =
            buffer.writeInt(value.value).let { Unit }

        override fun decode(buffer: ByteBuf) = CompanionEvent(buffer.readInt())
    }

    private class OtherEventCodec(override val eventId: String) : RedisEventCodec<OtherEvent> {
        override fun encode(buffer: ByteBuf, value: OtherEvent) = buffer.writeInt(value.value).let { Unit }
        override fun decode(buffer: ByteBuf) = OtherEvent(buffer.readInt())
    }
}
