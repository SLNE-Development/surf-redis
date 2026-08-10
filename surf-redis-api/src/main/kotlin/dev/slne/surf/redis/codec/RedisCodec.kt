package dev.slne.surf.redis.codec

import io.netty.buffer.ByteBuf

/**
 * Encodes and decodes values directly from Netty buffers.
 *
 * Implementations must be thread-safe: a single codec instance can be invoked concurrently by
 * event publishers, Redis listeners, and synchronized structures. Implementations must not retain
 * a supplied [ByteBuf] or change its reference count. The caller owns and releases every buffer.
 *
 * [codecId] and [version] form the wire-compatibility identity used by synchronized structures.
 * Override [codecId] when the codec class name is not stable across deployments (for example, for
 * anonymous or generated codec classes). Increment [version] whenever the encoded representation
 * changes incompatibly.
 *
 * ### Generic codecs must override [codecId]
 *
 * The default [codecId] is the codec's class name, which is identical for every type argument of a
 * generic codec: `JsonKotlinCodec<UserProfileV1>` and `JsonKotlinCodec<UserProfileV2>` produce the
 * same identity. A synchronized structure only compares this identity, so it cannot detect that the
 * stored payload type changed and will decode old data into the new type - silently defaulting any
 * removed or renamed field. If a codec class is reused for more than one value type, override
 * [codecId] to include the value type (for example, the serializer's serial name).
 *
 * @param T value type handled by this codec
 */
interface RedisCodec<T : Any> {
    /**
     * Stable identifier for this codec's wire format.
     */
    val codecId: String
        get() = javaClass.name

    /**
     * Version of the wire format produced by this codec. Must be positive.
     */
    val version: Int
        get() = 1

    /**
     * Writes [value] at the current writer index of [buffer].
     *
     * The implementation must not release or retain [buffer].
     */
    fun encode(buffer: ByteBuf, value: T)

    /**
     * Reads one value from the current reader index of [buffer].
     *
     * The implementation must not release, retain, or store [buffer].
     */
    fun decode(buffer: ByteBuf): T
}
