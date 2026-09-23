package dev.slne.surf.redis.request

import dev.slne.surf.api.core.serializer.java.uuid.JavaUUIDStringSerializer
import dev.slne.surf.redis.request.RequestResponseBusImpl.RequestEnvelope
import dev.slne.surf.redis.request.RequestResponseBusImpl.ResponseEnvelope
import dev.slne.surf.redis.util.KotlinSerializerCache
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.encoding.encodeStructure
import java.util.*

internal class OutboundEnvelope(@JvmField val requestId: UUID, @JvmField val payload: Any)

/**
 * Provides cached strategies that encode requests and responses directly into their JSON envelope.
 */
internal class OutboundEnvelopeSerializers(private val payloadSerializers: KotlinSerializerCache<Any>) {
    private val requestSerializers = envelopeSerializers(RequestEnvelope.serializer().descriptor)
    private val responseSerializers = envelopeSerializers(ResponseEnvelope.serializer().descriptor)

    private fun envelopeSerializers(
        envelopeDescriptor: SerialDescriptor,
    ): ClassValue<SerializationStrategy<OutboundEnvelope>?> {
        return object : ClassValue<SerializationStrategy<OutboundEnvelope>?>() {
            override fun computeValue(type: Class<*>): SerializationStrategy<OutboundEnvelope>? {
                val payloadSerializer = payloadSerializers.get(type) ?: return null
                return OutboundEnvelopeSerializer(envelopeDescriptor, type.name, payloadSerializer)
            }
        }
    }

    /**
     * @return the request envelope strategy for [requestType], or `null` if it has no Kotlin serializer
     */
    fun request(requestType: Class<out RedisRequest>): SerializationStrategy<OutboundEnvelope>? {
        return requestSerializers.get(requestType)
    }

    /**
     * @return the response envelope strategy for [responseType], or `null` if it has no Kotlin serializer
     */
    fun response(responseType: Class<out RedisResponse>): SerializationStrategy<OutboundEnvelope>? {
        return responseSerializers.get(responseType)
    }
}

/**
 * Encodes one payload type into an envelope described by [descriptor].
 */
private class OutboundEnvelopeSerializer(
    override val descriptor: SerialDescriptor,
    private val className: String,
    private val payloadSerializer: KSerializer<Any>
) : SerializationStrategy<OutboundEnvelope> {

    override fun serialize(encoder: Encoder, value: OutboundEnvelope) {
        encoder.encodeStructure(descriptor) {
            encodeSerializableElement(descriptor, 0, JavaUUIDStringSerializer, value.requestId)
            encodeStringElement(descriptor, 1, className)
            encodeSerializableElement(descriptor, 2, payloadSerializer, value.payload)
        }
    }
}
