package dev.slne.surf.redis.event

import dev.slne.surf.redis.util.KotlinSerializerCache
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.descriptors.element
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.encoding.decodeStructure
import kotlinx.serialization.encoding.encodeStructure
import kotlinx.serialization.modules.SerializersModule

/**
 * Provides cached serializers that encode and decode Redis events directly as their JSON envelope.
 *
 * Each serializer is created once per event class and handles the envelope and event payload in a
 * single pass, without an intermediate [kotlinx.serialization.json.JsonElement] tree.
 */
class EventEnvelopeSerializers(
    private val eventSerializers: KotlinSerializerCache<RedisEvent>
) {
    constructor(module: SerializersModule) : this(KotlinSerializerCache<RedisEvent>(module))

    private val envelopeSerializers = object : ClassValue<KSerializer<RedisEvent>?>() {
        override fun computeValue(type: Class<*>): KSerializer<RedisEvent>? {
            val eventSerializer = eventSerializers.get(type) ?: return null
            return EventEnvelopeSerializer(type.name, eventSerializer)
        }
    }

    /**
     * Returns the cached envelope serializer for [eventType].
     *
     * @return the serializer, or `null` if no Kotlin serializer is available for the event type
     */
    fun get(eventType: Class<out RedisEvent>): KSerializer<RedisEvent>? {
        return envelopeSerializers.get(eventType)
    }
}

/**
 * Encodes and decodes a single event type as an event envelope without materializing its payload.
 *
 * The descriptor intentionally models the envelope as a class so configured serialization
 * behavior such as JSON naming strategies is applied to its fields normally.
 *
 * Decoding accepts the envelope fields in any order and throws [SerializationException] if the
 * envelope names a different event class or lacks a field.
 */
private class EventEnvelopeSerializer(
    private val eventClass: String,
    private val eventSerializer: KSerializer<RedisEvent>
) : KSerializer<RedisEvent> {
    override val descriptor = buildClassSerialDescriptor("dev.slne.surf.redis.event.EventEnvelope") {
        element<String>("eventClass")
        element("eventData", eventSerializer.descriptor)
    }

    override fun serialize(encoder: Encoder, value: RedisEvent) {
        encoder.encodeStructure(descriptor) {
            encodeStringElement(descriptor, 0, eventClass)
            encodeSerializableElement(descriptor, 1, eventSerializer, value)
        }
    }

    override fun deserialize(decoder: Decoder): RedisEvent = decoder.decodeStructure(descriptor) {
        var hasClass = false
        var event: RedisEvent? = null
        while (true) {
            when (val index = decodeElementIndex(descriptor)) {
                0 -> {
                    val name = decodeStringElement(descriptor, 0)
                    if (name != eventClass) {
                        throw SerializationException("Expected event class '$eventClass' but was '$name'")
                    }
                    hasClass = true
                }

                1 -> event = decodeSerializableElement(descriptor, 1, eventSerializer)
                CompositeDecoder.DECODE_DONE -> break
                else -> throw SerializationException("Unexpected event envelope index: $index")
            }
        }

        if (!hasClass || event == null) {
            throw SerializationException("Event envelope for '$eventClass' is missing its class or data")
        }
        event
    }
}
