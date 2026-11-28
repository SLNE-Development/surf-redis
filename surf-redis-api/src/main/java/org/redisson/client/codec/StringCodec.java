/**
 * Copyright (c) 2013-2026 Nikita Koksharov
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 *    http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * <p>
 * Modified by surf-redis: the UTF-8 value encoder uses String#getBytes instead of
 * ByteBuf#writeCharSequence. Everything else is unchanged from Redisson 4.7.0.
 */
package org.redisson.client.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;
import org.redisson.codec.JsonCodec;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Redisson's {@code StringCodec}, patched by surf-redis and shaded in place of the original.
 * <p>
 * The public signature matches Redisson's class exactly, and the encoded bytes are identical to
 * Redisson's for every input and charset. For UTF-8, the JDK encoder produces the bytes whenever
 * its output equals Netty's, which holds for all well-formed UTF-16. Strings with an unpaired
 * surrogate, and all other charsets, take Redisson's original {@code writeCharSequence} path.
 * <p>
 * The fast path returns an unpooled heap buffer of exactly the encoded size. The fallback returns
 * a buffer from {@link ByteBufAllocator#DEFAULT}, as Redisson does.
 *
 * @author Nikita Koksharov
 *
 */
public class StringCodec extends BaseCodec implements JsonCodec {

    public static final StringCodec INSTANCE = new StringCodec();

    private static final byte QUESTION_MARK = '?';

    private final Charset charset;

    private final boolean utf8;

    private final Encoder encoder = in -> encode(in.toString());

    private final Decoder<Object> decoder = this::decode;

    public StringCodec() {
        this(CharsetUtil.UTF_8);
    }

    public StringCodec(ClassLoader classLoader) {
        this();
    }

    public StringCodec(String charsetName) {
        this(Charset.forName(charsetName));
    }

    public StringCodec(Charset charset) {
        this.charset = charset;
        this.utf8 = StandardCharsets.UTF_8.equals(charset);
    }

    @Override
    public Decoder<Object> getValueDecoder() {
        return decoder;
    }

    @Override
    public Encoder getValueEncoder() {
        return encoder;
    }

    @Override
    public Encoder getEncoder() {
        return encoder;
    }

    @Override
    public Decoder<Object> getDecoder() {
        return decoder;
    }

    private Object decode(ByteBuf buf, State state) {
        String str = buf.toString(charset);
        buf.readerIndex(buf.readableBytes());
        return str;
    }

    private ByteBuf encode(String value) {
        if (utf8) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            if (bytes.length == value.length() || !hasReplacedSurrogates(bytes, value)) {
                return Unpooled.wrappedBuffer(bytes);
            }
        }
        ByteBuf out = ByteBufAllocator.DEFAULT.buffer();
        out.writeCharSequence(value, charset);
        return out;
    }

    /**
     * Returns whether {@code String#getBytes} replaced an unpaired surrogate of {@code value}
     * with {@code '?'}.
     * <p>
     * Exact because no multi-byte UTF-8 sequence contains {@code 0x3F}: every {@code 0x3F} in
     * {@code bytes} is either a {@code '?'} of {@code value} or a replacement. The JDK and Netty
     * encoders only differ after such a replacement.
     */
    private static boolean hasReplacedSurrogates(byte[] bytes, String value) {
        int marks = 0;
        for (byte b : bytes) {
            if (b == QUESTION_MARK) marks++;
        }
        if (marks == 0) return false;

        int chars = 0;
        int index = value.indexOf('?');
        while (index >= 0) {
            chars++;
            index = value.indexOf('?', index + 1);
        }
        return marks != chars;
    }
}
