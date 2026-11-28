package dev.slne.surf.redis.codec

import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.ByteBufUtil
import org.redisson.client.codec.LongCodec
import org.redisson.client.codec.StringCodec
import java.lang.classfile.ClassFile
import java.lang.classfile.ClassModel
import java.lang.reflect.AccessFlag
import java.nio.charset.Charset
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Pins the patched `org.redisson.client.codec.StringCodec` in `src/main/java` to Redisson's
 * original: same public signature, and the same bytes as Redisson's encoder for every input.
 */
class PatchedStringCodecTest {
    @BeforeTest
    fun `the patched class shadows Redisson's original`() {
        assertTrue(
            StringCodec::class.java.declaredMethods.any { it.name == "hasReplacedSurrogates" },
            "StringCodec was loaded from ${StringCodec::class.java.protectionDomain.codeSource.location}",
        )
    }

    @Test
    fun `encodes every string shape byte-identically to Redisson`() {
        for (value in SHAPES) {
            assertIdentical(StringCodec.INSTANCE, Charsets.UTF_8, value)
        }
    }

    @Test
    fun `encodes all short strings over a surrogate-heavy alphabet byte-identically to Redisson`() {
        val checked = visitShortStrings { assertIdentical(StringCodec.INSTANCE, Charsets.UTF_8, it) }
        assertEquals(9_331, checked)
    }

    @Test
    fun `keeps Redisson's encoder for other charsets`() {
        for (charset in listOf(Charsets.ISO_8859_1, Charsets.US_ASCII, Charsets.UTF_16, Charsets.UTF_16LE)) {
            val codec = StringCodec(charset)
            for (value in SHAPES) assertIdentical(codec, charset, value)
            visitShortStrings { assertIdentical(codec, charset, it) }
        }
    }

    @Test
    fun `resolves UTF-8 given by name`() {
        visitShortStrings { assertIdentical(StringCodec("UTF-8"), Charsets.UTF_8, it) }
    }

    @Test
    fun `numeric codecs inherit the patched encoder`() {
        assertContentEquals("-42".toByteArray(), drain(LongCodec.INSTANCE.valueEncoder.encode(-42L)))
    }

    @Test
    fun `decodes what it encodes`() {
        val value = "Grüße 🌊 surf-redis"
        val encoded = StringCodec.INSTANCE.valueEncoder.encode(value)
        try {
            assertEquals(value, StringCodec.INSTANCE.valueDecoder.decode(encoded, null))
            assertEquals(0, encoded.readableBytes())
        } finally {
            encoded.release()
        }
    }

    @Test
    fun `has exactly the public and protected members of Redisson's class`() {
        val sources = javaClass.classLoader.getResources("org/redisson/client/codec/StringCodec.class").toList()
        val upstream = sources.single { it.protocol == "jar" }
        val patched = sources.single { it.protocol != "jar" }

        assertEquals(signature(upstream.readBytes()), signature(patched.readBytes()))
    }

    private fun signature(bytes: ByteArray): List<String> {
        val model: ClassModel = ClassFile.of().parse(bytes)
        fun visible(flags: Set<AccessFlag>) = AccessFlag.PUBLIC in flags || AccessFlag.PROTECTED in flags

        val header = listOf(
            "flags ${model.flags().flags().sorted()}",
            "extends ${model.superclass().map { it.asInternalName() }.orElse(null)}",
            "implements ${model.interfaces().map { it.asInternalName() }.sorted()}",
        )
        val fields = model.fields()
            .filter { visible(it.flags().flags()) }
            .map { "field ${it.flags().flags().sorted()} ${it.fieldName().stringValue()} ${it.fieldType().stringValue()}" }
        val methods = model.methods()
            .filter { visible(it.flags().flags()) }
            .map { "method ${it.flags().flags().sorted()} ${it.methodName().stringValue()}${it.methodType().stringValue()}" }
        return header + (fields + methods).sorted()
    }

    private fun assertIdentical(codec: StringCodec, charset: Charset, value: Any) {
        val expected = drain(redissonEncode(value, charset))
        val actual = drain(codec.valueEncoder.encode(value))
        if (!expected.contentEquals(actual)) {
            assertContentEquals(expected, actual, "$charset, chars ${value.toString().map { it.code }}")
        }
    }

    /** Redisson 4.7.0's `StringCodec` encoder, verbatim. */
    private fun redissonEncode(value: Any, charset: Charset): ByteBuf {
        val out = ByteBufAllocator.DEFAULT.buffer()
        out.writeCharSequence(value.toString(), charset)
        return out
    }

    private fun drain(buffer: ByteBuf): ByteArray = try {
        ByteBufUtil.getBytes(buffer)
    } finally {
        buffer.release()
    }

    private companion object {
        val SHAPES: List<Any> = listOf(
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

        /** Visits every string of up to five chars over an alphabet that covers each encoder branch. */
        fun visitShortStrings(action: (String) -> Unit): Int {
            val alphabet = charArrayOf('a', '?', 'é', '€', '\uD800', '\uDC00')
            var checked = 0
            fun visit(value: String) {
                action(value)
                checked++
                if (value.length == 5) return
                for (c in alphabet) visit(value + c)
            }
            visit("")
            return checked
        }
    }
}
