package dev.slne.surf.redis.util

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import org.redisson.client.codec.StringCodec
import org.redisson.client.protocol.Encoder
import java.nio.charset.StandardCharsets

/**
 * Faster variant of [StringCodec] which produces the same output
 */
internal object Utf8StringCodec : StringCodec() {
    private const val QUESTION_MARK = '?'.code.toByte()

    private val encoder = Encoder { value -> encode(value.toString()) }

    override fun getValueEncoder(): Encoder = encoder
    override fun getEncoder(): Encoder = encoder

    private fun encode(value: String): ByteBuf {
        val bytes = value.toByteArray(StandardCharsets.UTF_8)
        if (bytes.size == value.length || !hasReplacedSurrogates(bytes, value)) {
            return Unpooled.wrappedBuffer(bytes)
        }
        return StringCodec.INSTANCE.valueEncoder.encode(value)
    }

    /**
     * Returns whether `getBytes` replaced an unpaired surrogate of [value] with `?`.
     */
    private fun hasReplacedSurrogates(bytes: ByteArray, value: String): Boolean {
        var marks = 0
        for (b in bytes) if (b == QUESTION_MARK) marks++
        if (marks == 0) return false

        var chars = 0
        var index = value.indexOf('?')
        while (index >= 0) {
            chars++
            index = value.indexOf('?', index + 1)
        }
        return marks != chars
    }
}
