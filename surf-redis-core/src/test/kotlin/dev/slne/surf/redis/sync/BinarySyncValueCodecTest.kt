package dev.slne.surf.redis.sync

import dev.slne.surf.redis.codec.RedisCodec
import dev.slne.surf.redis.codec.RedisCodecException
import io.netty.buffer.ByteBuf
import java.util.Base64
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class BinarySyncValueCodecTest {
    @Test
    fun `round trips binary values through the Lua-safe representation`() {
        val codec = BinarySyncValueCodec(IntCodec, "SyncValue 'counter' value")

        val encoded = codec.encode(42)

        assertTrue(encoded.none { it == '\u0000' })
        assertEquals(42, codec.decode(encoded))
    }

    @Test
    fun `encodes byte-identically to url-safe unpadded base64 for every payload size`() {
        val codec = BinarySyncValueCodec(RawBytesCodec, "SyncMap 'corpus' value")
        val reference = Base64.getUrlEncoder().withoutPadding()
        val sizes = intArrayOf(0, 1, 2, 3, 4, 5, 16, 62, 63, 64, 65, 127, 128, 300, 4095, 4096)

        // Ascending, descending and repeated so the encoder is exercised with a buffer that is too
        // small, exactly right and far too large for the value at hand.
        repeat(3) {
            for (size in sizes + sizes.reversedArray()) {
                val payload = payloadOfSize(size)
                assertEquals(reference.encodeToString(payload), codec.encode(payload), "size $size")
                assertContentEquals(payload, codec.decode(codec.encode(payload)), "size $size")
            }
        }
    }

    @Test
    fun `codec descriptor is stable and versioned`() {
        val codec = BinarySyncValueCodec(IntCodec, "test")
        assertEquals("surf-codec-v1:8:test-int:3", codec.descriptor)
    }

    @Test
    fun `decode rejects unread trailing bytes with context`() {
        val codec = BinarySyncValueCodec(object : RedisCodec<Int> {
            override fun encode(buffer: ByteBuf, value: Int) {
                buffer.writeInt(value)
                buffer.writeByte(1)
            }

            override fun decode(buffer: ByteBuf): Int = buffer.readInt()
        }, "SyncList 'numbers' element")

        val failure = assertFailsWith<RedisCodecException> {
            codec.decode(codec.encode(5))
        }
        assertTrue(failure.message.orEmpty().contains("SyncList 'numbers' element"))
    }

    private fun payloadOfSize(size: Int) = ByteArray(size) { (it * 31 + 7).toByte() }.also {
        if (size >= 2) {
            it[0] = 0
            it[size - 1] = -1
        }
    }

    private object RawBytesCodec : RedisCodec<ByteArray> {
        override val codecId = "test-raw-bytes"
        override val version = 1

        override fun encode(buffer: ByteBuf, value: ByteArray) {
            buffer.writeBytes(value)
        }

        override fun decode(buffer: ByteBuf): ByteArray {
            val bytes = ByteArray(buffer.readableBytes())
            buffer.readBytes(bytes)
            return bytes
        }
    }

    private object IntCodec : RedisCodec<Int> {
        override val codecId = "test-int"
        override val version = 3

        override fun encode(buffer: ByteBuf, value: Int) {
            buffer.writeInt(value)
        }

        override fun decode(buffer: ByteBuf): Int = buffer.readInt()
    }
}
