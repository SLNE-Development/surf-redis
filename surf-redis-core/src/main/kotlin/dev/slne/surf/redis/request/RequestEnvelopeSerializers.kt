package dev.slne.surf.redis.request

import dev.slne.surf.api.core.serializer.java.uuid.JavaUUIDStringSerializer
import dev.slne.surf.redis.request.RequestResponseBusImpl.RequestEnvelope
import dev.slne.surf.redis.request.RequestResponseBusImpl.ResponseEnvelope
import dev.slne.surf.redis.util.KotlinSerializerCache
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.encoding.decodeStructure
import kotlinx.serialization.encoding.encodeStructure
import java.util.*

internal class RequestEnvelopeContent(@JvmField val requestId: UUID, @JvmField val payload: Any)

/**
 * Provides cached serializers that encode and decode requests and responses directly as their JSON
 * envelope, without an intermediate [kotlinx.serialization.json.JsonElement] tree for the payload.
 */
internal class RequestEnvelopeSerializers(private val payloadSerializers: KotlinSerializerCache<Any>) {
    private val requestSerializers = envelopeSerializers(RequestEnvelope.serializer().descriptor)
    private val responseSerializers = envelopeSerializers(ResponseEnvelope.serializer().descriptor)

    private fun envelopeSerializers(
        envelopeDescriptor: SerialDescriptor,
    ): ClassValue<KSerializer<RequestEnvelopeContent>?> {
        return object : ClassValue<KSerializer<RequestEnvelopeContent>?>() {
            override fun computeValue(type: Class<*>): KSerializer<RequestEnvelopeContent>? {
                val payloadSerializer = payloadSerializers.get(type) ?: return null
                return RequestEnvelopeSerializer(envelopeDescriptor, type.name, payloadSerializer)
            }
        }
    }

    /**
     * @return the request envelope serializer for [requestType], or `null` if it has no Kotlin serializer
     */
    fun request(requestType: Class<out RedisRequest>): KSerializer<RequestEnvelopeContent>? {
        return requestSerializers.get(requestType)
    }

    /**
     * @return the response envelope serializer for [responseType], or `null` if it has no Kotlin serializer
     */
    fun response(responseType: Class<out RedisResponse>): KSerializer<RequestEnvelopeContent>? {
        return responseSerializers.get(responseType)
    }
}

/**
 * Encodes and decodes one payload type as the envelope described by [descriptor].
 *
 * Decoding accepts the envelope fields in any order and throws [SerializationException] if the
 * envelope names a different class than [className] or lacks a field.
 */
private class RequestEnvelopeSerializer(
    override val descriptor: SerialDescriptor,
    private val className: String,
    private val payloadSerializer: KSerializer<Any>
) : KSerializer<RequestEnvelopeContent> {

    override fun serialize(encoder: Encoder, value: RequestEnvelopeContent) {
        encoder.encodeStructure(descriptor) {
            encodeSerializableElement(descriptor, 0, JavaUUIDStringSerializer, value.requestId)
            encodeStringElement(descriptor, 1, className)
            encodeSerializableElement(descriptor, 2, payloadSerializer, value.payload)
        }
    }

    override fun deserialize(decoder: Decoder): RequestEnvelopeContent = decoder.decodeStructure(descriptor) {
        var requestId: UUID? = null
        var hasClass = false
        var payload: Any? = null
        while (true) {
            when (val index = decodeElementIndex(descriptor)) {
                0 -> requestId = decodeSerializableElement(descriptor, 0, JavaUUIDStringSerializer)
                1 -> {
                    val name = decodeStringElement(descriptor, 1)
                    if (name != className) {
                        throw SerializationException("Expected envelope class '$className' but was '$name'")
                    }
                    hasClass = true
                }

                2 -> payload = decodeSerializableElement(descriptor, 2, payloadSerializer)
                CompositeDecoder.DECODE_DONE -> break
                else -> throw SerializationException("Unexpected envelope index: $index")
            }
        }

        if (requestId == null || !hasClass || payload == null) {
            throw SerializationException("Envelope for '$className' is missing its request id, class or data")
        }
        RequestEnvelopeContent(requestId, payload)
    }
}
