package dev.slne.surf.redis.util

import kotlinx.serialization.KSerializer
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.serializerOrNull

/**
 * Per-class cache of [KSerializer] instances.
 *
 * Backed by [ClassValue] for fast, GC-friendly lookups keyed by [Class]. Misses are cached as
 * a sentinel value so that repeated lookups for unserializable classes do not re-invoke
 * [SerializersModule.serializerOrNull] every time.
 */
class KotlinSerializerCache<T>(
    private val module: SerializersModule,
    private val type: Class<T>
) {

    private val cache = object : ClassValue<Any>() {
        @Suppress("UNCHECKED_CAST")
        override fun computeValue(type: Class<*>): Any {
            if (!this@KotlinSerializerCache.type.isAssignableFrom(type)) return MISSING
            return (module.serializerOrNull(type) as? KSerializer<T>) ?: MISSING
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun get(type: Class<*>): KSerializer<T>? {
        val value = cache.get(type)
        return if (value === MISSING) null else value as KSerializer<T>
    }

    companion object {
        private val MISSING: Any = Any()

        inline operator fun <reified T> invoke(
            module: SerializersModule,
        ): KotlinSerializerCache<T> = KotlinSerializerCache(module, T::class.java)
    }
}