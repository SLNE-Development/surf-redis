package dev.slne.surf.redis.codec

import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufUtil
import io.netty.buffer.Unpooled
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class RedisUtf8StringTest {
    @Test
    fun `round trips ascii and multi-byte text without a temporary buffer`() {
        val buffer = Unpooled.buffer()
        try {
            buffer.writeString("surf-redis")
            buffer.writeString("Greetings 🌊")

            assertEquals("surf-redis", buffer.readString())
            assertEquals("Greetings 🌊", buffer.readString())
            assertEquals(0, buffer.readableBytes())
        } finally {
            buffer.release()
        }
    }

    @Test
    fun `enforces character limit before writing`() {
        val buffer = Unpooled.buffer()
        try {
            assertFailsWith<Exception> {
                buffer.writeString("too long", maxLength = 3)
            }
            assertEquals(0, buffer.writerIndex())
        } finally {
            buffer.release()
        }
    }

    @Test
    fun `uses actual utf8 size instead of reserving worst case capacity`() {
        val value = "a".repeat(1024)
        val encodedSize = 2 + value.length // two-byte VarInt length prefix plus ASCII payload
        val buffer = Unpooled.buffer(encodedSize, encodedSize * 3)
        try {
            buffer.writeString(value, maxLength = value.length)

            assertEquals(encodedSize, buffer.writerIndex())
            assertEquals(encodedSize, buffer.capacity())
        } finally {
            buffer.release()
        }
    }

    @Test
    fun `ascii fast path stays byte-identical to the netty encoder`() {
        for ((name, value) in WIRE_CORPUS) {
            for ((target, newBuffer) in TARGETS) {
                val expected = encodeWithNetty(value, newBuffer)
                val actual = encodeWithRedisUtf8String(value, newBuffer)

                assertContentEquals(
                    expected,
                    actual,
                    "wire bytes differ for '$name' into a $target buffer: " +
                            "${expected.toHex()} != ${actual.toHex()}",
                )
            }
        }
    }

    @Test
    fun `ascii detection accepts exactly the ascii range`() {
        val allAscii = String(CharArray(0x80) { it.toChar() })
        assertEquals(true, RedisUtf8String.isAscii(allAscii))
        assertEquals(false, RedisUtf8String.isAscii(allAscii + Char(0x80)))

        for ((name, value) in WIRE_CORPUS) {
            assertEquals(
                value.all { it.code < 0x80 },
                RedisUtf8String.isAscii(value),
                "wrong encoder path selected for '$name'",
            )
        }
    }

    @Test
    fun `container readers reject attacker-controlled allocation sizes`() {
        val buffer = Unpooled.buffer()
        try {
            buffer.writeVarInt(Int.MAX_VALUE)
            assertFailsWith<Exception> {
                buffer.readVarIntArray()
            }
        } finally {
            buffer.release()
        }
    }
}

private const val HIGH_SURROGATE = 0xD83C
private const val LOW_SURROGATE = 0xDF0A
private const val TWO_BYTE = 0x00E9 // LATIN SMALL LETTER E WITH ACUTE
private const val THREE_BYTE = 0x20AC // EURO SIGN

private fun text(vararg codes: Int) = String(CharArray(codes.size) { codes[it].toChar() })

/**
 * Inputs whose encoding must not change, including the malformed-surrogate cases where Netty's
 * encoder and [String.toByteArray] disagree.
 */
private val WIRE_CORPUS: List<Pair<String, String>> = listOf(
    "empty" to "",
    "ascii" to "surf-redis",
    "nul" to text(0x00),
    "max ascii" to text(0x7F),
    // 0x3F is also Netty's replacement byte for a malformed surrogate, so the corpus must not rely
    // on that byte being unreachable through valid input.
    "question mark" to "?",
    "first two-byte" to text(0x80),
    "two-byte" to text(TWO_BYTE),
    "three-byte" to text(THREE_BYTE),
    "surrogate pair" to text(HIGH_SURROGATE, LOW_SURROGATE),
    "lone high surrogate" to text(HIGH_SURROGATE),
    "lone low surrogate" to text(LOW_SURROGATE),
    "high surrogate then ascii" to text(HIGH_SURROGATE, 'a'.code),
    // Netty charges two bytes for this pair and writes '?' plus the raw low byte of the second
    // character, so utf8Bytes == length holds while the bytes are neither valid UTF-8 nor what
    // String.toByteArray produces: a length-based ascii test would corrupt this case.
    "high surrogate then two-byte" to text(HIGH_SURROGATE, TWO_BYTE),
    "high surrogate then high surrogate" to text(HIGH_SURROGATE, HIGH_SURROGATE),
    "trailing high surrogate" to text('a'.code, HIGH_SURROGATE),
    "mixed" to text('a'.code, TWO_BYTE, THREE_BYTE, HIGH_SURROGATE, LOW_SURROGATE, '?'.code),
    "long ascii" to "a".repeat(4096),
    "max length ascii" to "b".repeat(RedisUtf8String.MAX_STRING_LENGTH),
    "long mixed" to text('a'.code, TWO_BYTE, THREE_BYTE).repeat(1024),
)

private const val ARRAY_OFFSET = 37
private const val SLICE_CAPACITY = 64 * 1024

/**
 * Buffer shapes the encoder must produce identical bytes into.
 *
 * The sliced target is not redundant: production packets are pooled slices whose backing array starts
 * at a nonzero offset, while a plain [Unpooled.buffer] always reports `arrayOffset() == 0` and so
 * cannot detect an encoder that ignores it.
 */
private val TARGETS: List<Pair<String, () -> ByteBuf>> = listOf(
    "heap" to { Unpooled.buffer() },
    "direct" to { Unpooled.directBuffer() },
    "heap at a nonzero array offset" to {
        val slice = Unpooled.buffer(ARRAY_OFFSET + SLICE_CAPACITY)
            .slice(ARRAY_OFFSET, SLICE_CAPACITY)
            .clear()
        check(slice.hasArray() && slice.arrayOffset() != 0) {
            "this target only tests anything while it is array-backed at a nonzero offset"
        }
        slice
    },
)

/** Encodes [value] exactly as [RedisUtf8String.write] did before the ascii fast path was added. */
private fun encodeWithNetty(value: String, newBuffer: () -> ByteBuf): ByteArray {
    val buffer = newBuffer()
    try {
        val bytesWritten = ByteBufUtil.utf8Bytes(value)
        buffer.writeVarInt(bytesWritten)
        ByteBufUtil.reserveAndWriteUtf8(buffer, value, bytesWritten)
        return ByteBufUtil.getBytes(buffer)
    } finally {
        buffer.release()
    }
}

private fun encodeWithRedisUtf8String(value: String, newBuffer: () -> ByteBuf): ByteArray {
    val buffer = newBuffer()
    try {
        buffer.writeString(value)
        return ByteBufUtil.getBytes(buffer)
    } finally {
        buffer.release()
    }
}

private fun ByteArray.toHex() = joinToString(" ") { "%02x".format(it) }
