package dev.slne.surf.redis.event

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Pins the quality of the outbound packet capacity hint: a hint that is too small forces
 * `AdaptiveByteBuf` to reallocate the pooled segment and copy the whole previous capacity, a hint
 * that is too large holds an oversized segment for the duration of the publish.
 */
class PacketEstimateTest {
    /** Steps skipped before scoring, so the hint has reached its steady state for the pattern. */
    private val warmup = 32

    @Test
    fun `a constant packet size is hinted exactly and never reallocates`() {
        val steps = PacketEstimateProbe.replay(IntArray(64) { 4_096 }).drop(warmup)

        assertEquals(emptyList(), steps.filter { it.reallocated })
        assertEquals(listOf(steps.first().packetSize), steps.map { it.estimateBefore }.distinct())
    }

    @Test
    fun `two alternating packet sizes never reallocate`() {
        val steps = PacketEstimateProbe.replay(PacketEstimateProbe.patterns.getValue("alternating"))
            .drop(warmup)

        assertEquals(emptyList(), steps.filter { it.reallocated })
        assertEquals(0L, steps.sumOf { it.copiedBytes.toLong() })
    }

    @Test
    fun `the hint returns to the small packet size shortly after a burst`() {
        val steps = PacketEstimateProbe.replay(PacketEstimateProbe.patterns.getValue("bursty"))
            .drop(warmup)
        val small = steps.filter { it.payloadBytes == 1_024 }
        val smallPacket = small.first().packetSize

        assertTrue(small.any { it.estimateBefore == smallPacket }, "hint never reached $smallPacket")
        assertTrue(
            small.count { it.estimateBefore > smallPacket } * 4 < small.size,
            "hint stayed above $smallPacket for ${small.count { it.estimateBefore > smallPacket }} of ${small.size} small packets"
        )
    }

    @Test
    fun `the hint recovers from a one-off outlier within a bounded number of records`() {
        val steady = 1_024
        val outlier = 1 shl 20
        val sizes = IntArray(32) { steady } + intArrayOf(outlier) + IntArray(32) { steady }
        val steps = PacketEstimateProbe.replay(sizes)
        val steadyPacket = steps.first().packetSize

        val afterOutlier = steps.drop(33)
        val recovered = afterOutlier.indexOfFirst { it.estimateBefore == steadyPacket }

        assertEquals(11, recovered, "records needed to return to a $steadyPacket-byte hint")
        assertTrue(afterOutlier.drop(recovered).all { it.estimateBefore == steadyPacket })
    }

    @Test
    fun `the hint stays within the codec limits`() {
        val sizes = intArrayOf(0, 1, 65_536, 0, 65_536, 1, 1, 1)
        val steps = PacketEstimateProbe.replay(sizes)

        assertTrue(steps.all { it.estimateBefore in 1..CustomEventPacketCodec.MAX_PACKET_SIZE })
    }
}
