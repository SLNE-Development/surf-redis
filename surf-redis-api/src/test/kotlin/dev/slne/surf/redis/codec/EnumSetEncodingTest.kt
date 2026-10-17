package dev.slne.surf.redis.codec

import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufUtil
import io.netty.buffer.Unpooled
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.enums.enumEntries
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/**
 * Pins [writeEnumSet] / [readEnumSet] to the `BitSet` encoding they replaced.
 *
 * Both are `inline`, so consumers compiled against an older api keep executing the `BitSet` body
 * ([legacyWriteEnumSet] / [legacyReadEnumSet]). Every assertion therefore runs in both directions.
 */
class EnumSetEncodingTest {
    @Test
    fun `encodes every subset like the BitSet encoding`() {
        assertAllSubsetsMatch<NoConstants>()
        assertAllSubsetsMatch<OneConstant>()
        assertAllSubsetsMatch<EightConstants>()
        assertAllSubsetsMatch<NineConstants>()
        assertAllSubsetsMatch<SixteenConstants>()
        assertAllSubsetsMatch<TimeUnit>()
    }

    @Test
    fun `encodes enums beyond one 64-bit word like the BitSet encoding`() {
        val values = enumEntries<SeventyConstants>()
        val subsets = buildList {
            add(EnumSet.noneOf(SeventyConstants::class.java))
            add(EnumSet.allOf(SeventyConstants::class.java))
            for (ordinal in intArrayOf(0, 7, 8, 63, 64, 65, values.lastIndex)) {
                add(EnumSet.of(values[ordinal]))
                add(EnumSet.complementOf(EnumSet.of(values[ordinal])))
            }
            val random = Random(15)
            repeat(2_000) {
                add(values.filterTo(EnumSet.noneOf(SeventyConstants::class.java)) { random.nextBoolean() })
            }
        }

        subsets.forEach { assertMatchesLegacy(it) }
    }

    @Test
    fun `encoding depends only on membership, not on the set implementation`() {
        val values = enumEntries<SixteenConstants>()
        val members = values.filterIndexed { i, _ -> i % 3 != 1 }
        val expected = encode { it.legacyWriteEnumSet(EnumSet.copyOf(members)) }

        for (set in listOf(HashSet(members), LinkedHashSet(members.reversed()), TreeSet(members), members.toSet())) {
            assertContentEquals(expected, encode { it.writeEnumSet(set) }, set.javaClass.name)
        }
    }

    @Test
    fun `ignores padding bits past the last constant like the BitSet decoder`() {
        assertDecodesLikeLegacy(allBitsSet(byteCount = 2), EnumSet.allOf(NineConstants::class.java))
        assertDecodesLikeLegacy(allBitsSet(byteCount = 9), EnumSet.allOf(SeventyConstants::class.java))
    }

    @Test
    fun `truncated input fails without consuming it like the BitSet decoder`() {
        val legacy = Unpooled.wrappedBuffer(byteArrayOf(1))
        val current = Unpooled.wrappedBuffer(byteArrayOf(1))

        val legacyFailure = assertFailsWith<IndexOutOfBoundsException> { legacy.legacyReadEnumSet<SixteenConstants>() }
        val currentFailure = assertFailsWith<IndexOutOfBoundsException> { current.readEnumSet<SixteenConstants>() }

        assertEquals(legacyFailure.message, currentFailure.message)
        assertEquals(0, legacy.readerIndex())
        assertEquals(0, current.readerIndex())
    }

    @Test
    fun `full buffer fails without writing like the BitSet encoding`() {
        val set = EnumSet.allOf(SixteenConstants::class.java)
        val legacy = Unpooled.buffer(1, 1)
        val current = Unpooled.buffer(1, 1)
        try {
            val legacyFailure = assertFailsWith<IndexOutOfBoundsException> { legacy.legacyWriteEnumSet(set) }
            val currentFailure = assertFailsWith<IndexOutOfBoundsException> { current.writeEnumSet(set) }

            assertEquals(legacyFailure.message, currentFailure.message)
            assertEquals(0, legacy.writerIndex())
            assertEquals(0, current.writerIndex())
        } finally {
            legacy.release()
            current.release()
        }
    }

    @Test
    fun `failing membership check leaves the writer index unchanged like the BitSet encoding`() {
        val members = EnumSet.of(SixteenConstants.A)
        val failing = object : kotlin.collections.AbstractSet<SixteenConstants>() {
            override val size get() = members.size
            override fun iterator() = members.iterator()
            override fun contains(element: SixteenConstants): Boolean {
                check(element != SixteenConstants.J) { "membership check failed" }
                return element in members
            }
        }

        for (write in listOf<(ByteBuf) -> Unit>({ it.legacyWriteEnumSet(failing) }, { it.writeEnumSet(failing) })) {
            val buffer = Unpooled.buffer()
            try {
                buffer.writeByte(PREFIX)
                assertFailsWith<IllegalStateException> { write(buffer) }
                assertEquals(1, buffer.writerIndex())
            } finally {
                buffer.release()
            }
        }
    }

    private inline fun <reified E : Enum<E>> assertAllSubsetsMatch() {
        val values = enumEntries<E>()
        for (mask in 0 until (1 shl values.size)) {
            assertMatchesLegacy(
                values.filterIndexedTo(EnumSet.noneOf(E::class.java)) { i, _ -> mask and (1 shl i) != 0 }
            )
        }
    }

    private inline fun <reified E : Enum<E>> assertMatchesLegacy(set: EnumSet<E>) {
        val legacy = encode { it.legacyWriteEnumSet(set) }
        val current = encode { it.writeEnumSet(set) }

        assertContentEquals(legacy, current, "bytes differ for ${E::class.simpleName} $set")
        assertDecodesLikeLegacy(legacy, set)
    }

    private inline fun <reified E : Enum<E>> assertDecodesLikeLegacy(framed: ByteArray, expected: Set<E>) {
        assertEquals(expected, decode(framed) { it.legacyReadEnumSet<E>() }, "legacy read of ${E::class.simpleName}")
        assertEquals(expected, decode(framed) { it.readEnumSet<E>() }, "current read of ${E::class.simpleName}")
    }

    /** Encodes between two marker bytes into a buffer that must grow, so offsets and growth are covered. */
    private inline fun encode(body: (ByteBuf) -> Unit): ByteArray {
        val buffer = Unpooled.buffer(1)
        try {
            buffer.writeByte(PREFIX)
            body(buffer)
            buffer.writeByte(SUFFIX)
            return ByteBufUtil.getBytes(buffer)
        } finally {
            buffer.release()
        }
    }

    /** A framed encoding whose body and marker bytes are all `0xFF`. */
    private fun allBitsSet(byteCount: Int) = ByteArray(byteCount + 2) { 0xFF.toByte() }

    private inline fun <T> decode(framed: ByteArray, body: (ByteBuf) -> T): T {
        val buffer = Unpooled.wrappedBuffer(framed)
        buffer.skipBytes(1)
        val result = body(buffer)
        assertEquals(1, buffer.readableBytes(), "decoder consumed the wrong number of bytes")
        return result
    }

    private companion object {
        const val PREFIX = 0x5A
        const val SUFFIX = 0xA5
    }
}

/** The pre-KJO-015 `writeEnumSet` body, verbatim. */
private inline fun <reified E : Enum<E>> ByteBuf.legacyWriteEnumSet(set: Set<E>) {
    val values = enumEntries<E>()
    val bitSet = BitSet(values.size)

    for (i in values.indices) {
        bitSet.set(i, values[i] in set)
    }

    writeFixedBitSet(bitSet, values.size)
}

/** The pre-KJO-015 `readEnumSet` body, verbatim. */
private inline fun <reified E : Enum<E>> ByteBuf.legacyReadEnumSet(): EnumSet<E> {
    val values = enumEntries<E>()
    val bitSet = readFixedBitSet(values.size)
    val result = EnumSet.noneOf(E::class.java)

    for (i in values.indices) {
        if (bitSet.get(i)) {
            result.add(values[i])
        }
    }

    return result
}

private enum class NoConstants

private enum class OneConstant { A }

private enum class EightConstants { A, B, C, D, E, F, G, H }

private enum class NineConstants { A, B, C, D, E, F, G, H, I }

private enum class SixteenConstants { A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P }

private enum class SeventyConstants {
    V00, V01, V02, V03, V04, V05, V06, V07, V08, V09, V10, V11, V12, V13, V14, V15, V16, V17, V18, V19,
    V20, V21, V22, V23, V24, V25, V26, V27, V28, V29, V30, V31, V32, V33, V34, V35, V36, V37, V38, V39,
    V40, V41, V42, V43, V44, V45, V46, V47, V48, V49, V50, V51, V52, V53, V54, V55, V56, V57, V58, V59,
    V60, V61, V62, V63, V64, V65, V66, V67, V68, V69,
}
