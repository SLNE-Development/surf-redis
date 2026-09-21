package dev.slne.surf.redis.event

import dev.slne.surf.redis.codec.RedisCodecException
import dev.slne.surf.redis.codec.readString
import dev.slne.surf.redis.codec.writeString
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import java.nio.ReadOnlyBufferException
import kotlin.test.*

class CustomEventPacketCodecTest {
    @Test
    fun `string payloads remain read only and independently decodable after transport release`() {
        for (text in listOf("", "a".repeat(65_536), "Grüße 世界 🌊", "\u0000")) {
            var decodedBuffer: ByteBuf? = null
            val codec = object : RedisEventCodec<TextEvent> {
                override val eventId = "test:text-event"
                override fun encode(buffer: ByteBuf, value: TextEvent) = buffer.writeString(value.text, 100_000)
                override fun decode(buffer: ByteBuf): TextEvent {
                    decodedBuffer = buffer
                    assertTrue(buffer.isReadOnly)
                    assertFalse(buffer.hasArray())
                    assertFailsWith<ReadOnlyBufferException> { buffer.setByte(0, 0) }
                    assertFailsWith<ReadOnlyBufferException> { buffer.array() }
                    assertFailsWith<IndexOutOfBoundsException> {
                        buffer.toString(buffer.capacity(), 1, Charsets.UTF_8)
                    }
                    return TextEvent(buffer.readString(100_000))
                }
            }
            val registry = EventCodecRegistry().apply {
                registerExplicit(TextEvent::class.java, codec)
                freeze()
            }
            val registration = registry.codecForEventId(codec.eventId)!!
            val wire = CustomEventPacketCodec.redisCodec.valueEncoder.encode(
                CustomEventPacketCodec.outbound(TextEvent(text), registration)
            )
            val inbound = try {
                assertIs<CustomEventPacketCodec.InboundMessage.Packet>(
                    CustomEventPacketCodec.redisCodec.valueDecoder.decode(wire, null)
                )
            } finally {
                wire.release()
            }
            repeat(2) {
                val decoded = assertIs<CustomEventPacketCodec.DecodeResult.Event>(
                    inbound.decode(registry::codecForEventId)
                )
                assertEquals(text, (decoded.event as TextEvent).text)
                assertEquals(0, decodedBuffer!!.refCnt())
            }
        }
    }

    @Test
    fun `unread payload still fails and releases the decoding buffer`() {
        var decodedBuffer: ByteBuf? = null
        val registry = EventCodecRegistry().apply {
            registerExplicit(PacketEvent::class.java, object : RedisEventCodec<PacketEvent> {
                override val eventId = PacketCodec.EVENT_ID
                override fun encode(buffer: ByteBuf, value: PacketEvent) { buffer.writeInt(value.value) }
                override fun decode(buffer: ByteBuf): PacketEvent {
                    decodedBuffer = buffer
                    return PacketEvent(0)
                }
            })
            freeze()
        }
        val wire = encode(PacketEvent(7), registry.codecForEventId(PacketCodec.EVENT_ID)!!)
        try {
            val result = assertIs<CustomEventPacketCodec.DecodeResult.Failure>(decode(wire, registry))
            assertTrue(result.exception.message.orEmpty().contains("left 4 unread bytes"))
            assertEquals(0, decodedBuffer!!.refCnt())
        } finally {
            wire.release()
        }
    }

    @Test
    fun `custom event packet round trips on the isolated binary protocol`() {
        val registry = registry(PacketCodec(version = 4))
        val registration = registry.codecForEventId(PacketCodec.EVENT_ID)!!
        val original = PacketEvent(91)

        val packet = encode(original, registration)
        val firstByte = packet.getByte(packet.readerIndex())
        val result = try {
            assertIs<CustomEventPacketCodec.DecodeResult.Event>(decode(packet, registry))
        } finally {
            packet.release()
        }

        assertEquals(91, (result.event as PacketEvent).value)
        assertEquals(original.timestamp, result.timestamp)
        assertEquals(4, result.registration.version)
        assertEquals("surf-redis:events:binary", CustomEventPacketCodec.CHANNEL)
        assertNotEquals('{'.code.toByte(), firstByte)
    }

    @Test
    fun `missing codec is isolated after routing metadata is decoded`() {
        val registry = registry(PacketCodec())
        val registration = registry.codecForEventId(PacketCodec.EVENT_ID)!!
        val packet = encode(PacketEvent(7), registration)

        val result = try {
            assertIs<CustomEventPacketCodec.DecodeResult.MissingCodec>(decode(packet) { null })
        } finally {
            packet.release()
        }

        assertEquals(PacketCodec.EVENT_ID, result.eventId)
        assertEquals(1, result.codecVersion)
    }

    @Test
    fun `codec version mismatch does not invoke the decoder`() {
        val sending = registry(PacketCodec(version = 1))
        val receiving = registry(PacketCodec(version = 2))
        val packet = encode(PacketEvent(8), sending.codecForEventId(PacketCodec.EVENT_ID)!!)

        val result = try {
            assertIs<CustomEventPacketCodec.DecodeResult.VersionMismatch>(decode(packet, receiving))
        } finally {
            packet.release()
        }

        assertEquals(1, result.receivedVersion)
        assertEquals(2, result.registration.version)
    }

    @Test
    fun `one inbound packet is decoded independently by every subscriber registry`() {
        val publisher = registry(PacketCodec())
        val registration = publisher.codecForEventId(PacketCodec.EVENT_ID)!!
        val subscriberWithCodec = registry(PacketCodec())
        val subscriberWithoutCodec = EventCodecRegistry().apply { freeze() }
        val packet = encode(PacketEvent(3, ByteArray(64) { it.toByte() }), registration)

        val inbound = try {
            assertIs<CustomEventPacketCodec.InboundMessage.Packet>(
                CustomEventPacketCodec.redisCodec.valueDecoder.decode(packet, null)
            )
        } finally {
            packet.release()
        }

        val decoded = assertIs<CustomEventPacketCodec.DecodeResult.Event>(
            inbound.decode(subscriberWithCodec::codecForEventId)
        )
        val missing = assertIs<CustomEventPacketCodec.DecodeResult.MissingCodec>(
            inbound.decode(subscriberWithoutCodec::codecForEventId)
        )
        val decodedAgain = assertIs<CustomEventPacketCodec.DecodeResult.Event>(
            inbound.decode(subscriberWithCodec::codecForEventId)
        )

        assertEquals(3, (decoded.event as PacketEvent).value)
        assertContentEquals(ByteArray(64) { it.toByte() }, (decoded.event as PacketEvent).payload)
        assertEquals(PacketCodec.EVENT_ID, missing.eventId)
        assertEquals(3, (decodedAgain.event as PacketEvent).value)
        assertNotSame(decoded.event, decodedAgain.event)
    }

    @Test
    fun `serialization failure includes codec and event context`() {
        val codec = object : RedisEventCodec<PacketEvent> {
            override val eventId = "test:failing-event"
            override val codecId = "test-failing-codec"
            override fun encode(buffer: ByteBuf, value: PacketEvent) = error("boom")
            override fun decode(buffer: ByteBuf) = PacketEvent(0)
        }
        val registry = EventCodecRegistry().apply {
            registerExplicit(PacketEvent::class.java, codec)
            freeze()
        }

        val failure = assertFailsWith<RedisCodecException> {
            CustomEventPacketCodec.redisCodec.valueEncoder.encode(
                CustomEventPacketCodec.outbound(
                    PacketEvent(1),
                    registry.codecForEventId(codec.eventId)!!
                )
            )
        }

        assertTrue(failure.message.orEmpty().contains("test-failing-codec"))
        assertTrue(failure.message.orEmpty().contains(PacketEvent::class.java.name))
    }

    @Test
    fun `decoder failure surfaces as a failure result with codec context`() {
        val registration = registry(PacketCodec()).codecForEventId(PacketCodec.EVENT_ID)!!
        val packet = encode(PacketEvent(5), registration)
        val failingRegistry = EventCodecRegistry().apply {
            registerExplicit(PacketEvent::class.java, object : RedisEventCodec<PacketEvent> {
                override val eventId = PacketCodec.EVENT_ID
                override val codecId = "test-broken-decoder"
                override fun encode(buffer: ByteBuf, value: PacketEvent) = Unit
                override fun decode(buffer: ByteBuf): PacketEvent = error("cannot decode")
            })
            freeze()
        }

        val result = try {
            assertIs<CustomEventPacketCodec.DecodeResult.Failure>(decode(packet, failingRegistry))
        } finally {
            packet.release()
        }

        assertTrue(result.exception.message.orEmpty().contains("test-broken-decoder"))
    }

    @Test
    fun `redisson decoder reports malformed buffers as failure results`() {
        val malformed = Unpooled.wrappedBuffer(byteArrayOf(1, 2, 3))

        val result = try {
            assertIs<CustomEventPacketCodec.DecodeResult.Failure>(decode(malformed) { null })
        } finally {
            malformed.release()
        }

        assertTrue(result.exception.message.orEmpty().contains("Malformed"))
    }

    @Test
    fun `packet size estimate adapts without sharing redisson buffers`() {
        val registry = registry(PacketCodec())
        val registration = registry.codecForEventId(PacketCodec.EVENT_ID)!!

        val first = encode(PacketEvent(42, ByteArray(1_024)), registration)
        val firstSize = first.readableBytes()
        assertEquals(firstSize, registration.packetSizeEstimate)

        val second = encode(PacketEvent(42, ByteArray(4_096)), registration)
        val secondSize = second.readableBytes()
        assertNotSame(first, second)
        assertEquals(secondSize, registration.packetSizeEstimate)

        first.release()
        val result = try {
            assertIs<CustomEventPacketCodec.DecodeResult.Event>(decode(second, registry))
        } finally {
            second.release()
        }
        val event = result.event as PacketEvent
        assertEquals(42, event.value)
        assertEquals(4_096, event.payload.size)
    }

    private fun encode(event: PacketEvent, registration: EventCodecRegistration): ByteBuf =
        CustomEventPacketCodec.redisCodec.valueEncoder.encode(CustomEventPacketCodec.outbound(event, registration))

    private fun decode(packet: ByteBuf, registry: EventCodecRegistry): CustomEventPacketCodec.DecodeResult =
        decode(packet, registry::codecForEventId)

    private fun decode(
        packet: ByteBuf,
        resolver: (String) -> EventCodecRegistration?
    ): CustomEventPacketCodec.DecodeResult =
        when (val message = CustomEventPacketCodec.redisCodec.valueDecoder.decode(packet, null)) {
            is CustomEventPacketCodec.InboundMessage.Packet -> message.decode(resolver)
            is CustomEventPacketCodec.InboundMessage.Malformed ->
                CustomEventPacketCodec.DecodeResult.Failure(message.exception)

            else -> fail("unexpected decoder output: $message")
        }

    private fun registry(codec: PacketCodec): EventCodecRegistry = EventCodecRegistry().apply {
        registerExplicit(PacketEvent::class.java, codec)
        freeze()
    }

    private class TextEvent(val text: String) : RedisEvent()

    private class PacketEvent(
        val value: Int,
        val payload: ByteArray = byteArrayOf()
    ) : RedisEvent()

    private class PacketCodec(
        override val version: Int = 1
    ) : RedisEventCodec<PacketEvent> {
        override val eventId = EVENT_ID
        override val codecId = "test-packet-codec"

        override fun encode(buffer: ByteBuf, value: PacketEvent) {
            buffer.writeInt(value.value)
            buffer.writeBytes(value.payload)
        }

        override fun decode(buffer: ByteBuf): PacketEvent {
            val value = buffer.readInt()
            val payload = ByteArray(buffer.readableBytes())
            buffer.readBytes(payload)
            return PacketEvent(value, payload)
        }

        companion object {
            const val EVENT_ID = "test:packet-event"
        }
    }
}
