package dev.slne.surf.redis.event

import dev.slne.surf.redis.codec.writeString
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufAllocator

/**
 * Replays a sequence of payload sizes through the real [EventCodecRegistration] capacity estimate and
 * reports, per step, the hint the encoder would have started from and whether a pooled buffer of that
 * size has to be reallocated to hold the packet.
 *
 * `ByteBufAllocator.DEFAULT` is `AdaptiveByteBufAllocator` on this runtime; its
 * `AdaptiveByteBuf.capacity` copies the whole previous capacity rather than the written bytes, so
 * [Step.copiedBytes] is the memcpy an encode of that step pays. Reallocation is observed directly by
 * comparing the buffer's chunk and start index around the growth.
 */
internal object PacketEstimateProbe {
    data class Step(
        val payloadBytes: Int,
        val packetSize: Int,
        val estimateBefore: Int,
        val reallocated: Boolean,
        val copiedBytes: Int,
    )

    /** The payload patterns of `VariableEventPacketBenchmark`. */
    val patterns: Map<String, IntArray> = mapOf(
        "alternating" to IntArray(256) { if (it and 1 == 0) 256 else 65_536 },
        "bursty" to IntArray(256) { if (it % 32 == 31) 65_536 else 1_024 },
        "mixed" to intArrayOf(0, 32, 256, 1_024, 4_096, 16_384, 65_536)
            .let { sizes -> IntArray(256) { sizes[(it * 73 + 19) % sizes.size] } },
    )

    fun replay(payloadSizes: IntArray): List<Step> {
        val registration = registration()
        val packetSizes = payloadSizes.distinct().associateWith(::packetSizeFor)
        return payloadSizes.map { payloadBytes ->
            val packetSize = packetSizes.getValue(payloadBytes)
            val estimate = registration.packetSizeEstimate
                .coerceIn(1, CustomEventPacketCodec.MAX_PACKET_SIZE)
            val step = measure(payloadBytes, packetSize, estimate)
            registration.recordPacketSize(packetSize)
            step
        }
    }

    /** Packet size the binary codec produces for a [payloadBytes]-byte ASCII payload. */
    fun packetSizeFor(payloadBytes: Int): Int {
        val encoded = CustomEventPacketCodec.redisCodec.valueEncoder
            .encode(CustomEventPacketCodec.outbound(ProbeEvent(payload(payloadBytes)), registration()))
        return try {
            encoded.readableBytes()
        } finally {
            encoded.release()
        }
    }

    private fun measure(payloadBytes: Int, packetSize: Int, estimate: Int): Step {
        val buffer = ByteBufAllocator.DEFAULT.heapBuffer(estimate, CustomEventPacketCodec.MAX_PACKET_SIZE)
        return try {
            val segment = unwrapped(buffer)
            val chunkBefore = chunkField.get(segment)
            val startBefore = startIndexField.getInt(segment)
            val capacityBefore = buffer.capacity()
            buffer.ensureWritable(packetSize)
            val moved = chunkField.get(segment) !== chunkBefore || startIndexField.getInt(segment) != startBefore
            Step(payloadBytes, packetSize, estimate, moved, if (moved) capacityBefore else 0)
        } finally {
            buffer.release()
        }
    }

    private fun registration(): EventCodecRegistration {
        @Suppress("UNCHECKED_CAST")
        return EventCodecRegistration(
            eventType = ProbeEvent::class.java,
            codec = ProbeCodec as RedisEventCodec<RedisEvent>,
            eventId = ProbeCodec.eventId,
            version = 1,
            explicit = true,
        )
    }

    private fun payload(size: Int) = buildString(size) {
        repeat(size) { append(('a'.code + it % 26).toChar()) }
    }

    /** Leak detection samples buffers and hands back a `SimpleLeakAwareByteBuf` wrapper. */
    private tailrec fun unwrapped(buffer: ByteBuf): ByteBuf =
        if (adaptiveByteBuf.isInstance(buffer)) buffer
        else unwrapped(requireNonNull(buffer.unwrap()) { "not an AdaptiveByteBuf: ${buffer.javaClass.name}" })

    private inline fun <T : Any> requireNonNull(value: T?, message: () -> String): T =
        value ?: error(message())

    private val adaptiveByteBuf = Class.forName("io.netty.buffer.AdaptivePoolingAllocator\$AdaptiveByteBuf")
    private val chunkField = adaptiveByteBuf.getDeclaredField("chunk").apply { isAccessible = true }
    private val startIndexField = adaptiveByteBuf.getDeclaredField("startIndex").apply { isAccessible = true }

    class ProbeEvent(val payload: String) : RedisEvent()

    private object ProbeCodec : RedisEventCodec<ProbeEvent> {
        override val eventId = "estimate-probe"
        override val codecId = "estimate-probe-v1"
        override fun encode(buffer: ByteBuf, value: ProbeEvent) = buffer.writeString(value.payload, 1 shl 20)
        override fun decode(buffer: ByteBuf) = ProbeEvent("")
    }
}
