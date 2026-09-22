package dev.slne.surf.redis.util

import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufUtil
import org.redisson.client.codec.BaseCodec
import org.redisson.client.codec.StringCodec
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class Utf8StringCodecTest {
    @Test
    fun `encodes every string shape byte-identically to StringCodec`() {
        val values = listOf(
            "",
            "surf-redis:sync:map:players",
            "a?b=c",
            "\u0000",
            "Grüße aus Köln §a",
            "Preis: 5 €",
            "Welle 🌊",
            "Welle 🌊?",
            "\uD800a",
            "\uDC00",
            "a\uD800",
            "\uD800\uD800",
            "\uD800\uD800𐀀",
            "\uD800é",
            "\uD800€",
            "\uD800𐀀",
            "a?\uD800é",
            "x".repeat(70_000),
            "Grüße 🌊 € ? ".repeat(7_000),
            42L,
            7,
        )
        for (value in values) {
            assertContentEquals(stringCodecBytes(value), bytes(value), "value '$value'")
        }
    }

    @Test
    fun `encodes all short strings over a surrogate-heavy alphabet byte-identically to StringCodec`() {
        val alphabet = charArrayOf('a', '?', 'é', '€', '\uD800', '\uDC00')
        var checked = 0
        fun visit(value: String) {
            assertContentEquals(stringCodecBytes(value), bytes(value), "chars ${value.map { it.code }}")
            checked++
            if (value.length == 5) return
            for (c in alphabet) visit(value + c)
        }
        visit("")
        assertEquals(9_331, checked)
    }

    @Test
    fun `decodes what it encodes`() {
        val value = "Grüße 🌊 surf-redis"
        val encoded = Utf8StringCodec.valueEncoder.encode(value)
        try {
            assertEquals(value, Utf8StringCodec.valueDecoder.decode(encoded, null))
        } finally {
            encoded.release()
        }
    }

    @Test
    fun `is exempt from Redisson's per-class-loader codec copies`() {
        assertTrue(BaseCodec.SKIPPED_CODECS.any { it.isInstance(Utf8StringCodec) })
    }

    private fun bytes(value: Any): ByteArray = drain(Utf8StringCodec.valueEncoder.encode(value))

    private fun stringCodecBytes(value: Any): ByteArray = drain(StringCodec.INSTANCE.valueEncoder.encode(value))

    private fun drain(buffer: ByteBuf): ByteArray = try {
        ByteBufUtil.getBytes(buffer)
    } finally {
        buffer.release()
    }
}
