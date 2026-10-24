package dev.slne.surf.redis.request

import dev.slne.surf.api.core.serializer.java.uuid.JavaUUIDStringSerializer
import dev.slne.surf.redis.util.JsonEnvelopeHead
import dev.slne.surf.redis.util.KotlinSerializerCache
import kotlinx.serialization.Contextual
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNamingStrategy
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.contextual
import java.util.*
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertSame

/**
 * Pins that [RequestEnvelopeSerializers] writes exactly the text of the
 * [RequestResponseBusImpl.RequestEnvelope] / [RequestResponseBusImpl.ResponseEnvelope] data classes
 * and decodes it to the same payload as those data classes do.
 */
class RequestEnvelopeSerializersTest {

    /** Mirrors `RedisApi.createJson`. */
    @OptIn(ExperimentalSerializationApi::class)
    private val json = Json {
        ignoreUnknownKeys = true
        namingStrategy = JsonNamingStrategy.SnakeCase
        encodeDefaults = true
        serializersModule = SerializersModule { contextual(JavaUUIDStringSerializer) }
    }

    private val serializers = RequestEnvelopeSerializers(KotlinSerializerCache<Any>(json.serializersModule))

    private val fixedId = UUID.fromString("6f2b6a1e-6a5a-4b1e-9f0e-6d3f0a1b2c3d")

    private fun <R : RedisRequest> assertRequestMatchesEnvelope(request: R, serializer: KSerializer<R>, id: UUID = fixedId) {
        val expected = json.encodeToString(
            RequestResponseBusImpl.RequestEnvelope.serializer(),
            RequestResponseBusImpl.RequestEnvelope(id, request.javaClass.name, json.encodeToJsonElement(serializer, request))
        )
        val envelopeSerializer = serializers.request(request.javaClass)!!
        val streaming = json.encodeToString(envelopeSerializer, RequestEnvelopeContent(id, request))
        assertEquals(expected, streaming)

        val legacy = json.decodeFromString(RequestResponseBusImpl.RequestEnvelope.serializer(), streaming)
        val decoded = json.decodeFromString(envelopeSerializer, streaming)
        assertEquals(id, decoded.requestId)
        assertEquals<Class<*>>(request.javaClass, decoded.payload.javaClass)
        assertEquals(legacy.requestData, json.encodeToJsonElement(serializer, request.javaClass.cast(decoded.payload)))
        assertEquals(streaming, json.encodeToString(envelopeSerializer, decoded))
    }

    private fun <R : RedisResponse> assertResponseMatchesEnvelope(response: R, serializer: KSerializer<R>, id: UUID = fixedId) {
        val expected = json.encodeToString(
            RequestResponseBusImpl.ResponseEnvelope.serializer(),
            RequestResponseBusImpl.ResponseEnvelope(id, response.javaClass.name, json.encodeToJsonElement(serializer, response))
        )
        val envelopeSerializer = serializers.response(response.javaClass)!!
        val streaming = json.encodeToString(envelopeSerializer, RequestEnvelopeContent(id, response))
        assertEquals(expected, streaming)

        val legacy = json.decodeFromString(RequestResponseBusImpl.ResponseEnvelope.serializer(), streaming)
        val decoded = json.decodeFromString(envelopeSerializer, streaming)
        assertEquals(id, decoded.requestId)
        assertEquals<Class<*>>(response.javaClass, decoded.payload.javaClass)
        assertEquals(legacy.responseData, json.encodeToJsonElement(serializer, response.javaClass.cast(decoded.payload)))
        assertEquals(streaming, json.encodeToString(envelopeSerializer, decoded))
    }

    @Test
    fun `scalar payloads match envelope encoding`() {
        assertRequestMatchesEnvelope(ScalarRequest(), ScalarRequest.serializer())
        assertResponseMatchesEnvelope(ScalarResponse(), ScalarResponse.serializer())
    }

    @Test
    fun `nested, collection, null and empty payloads match envelope encoding`() {
        val full = ComplexRequest(Nested("inner", false), listOf("a", "b"), mapOf("x" to 1), Nested("p", true))
        val empty = ComplexRequest(Nested("", true), emptyList(), emptyMap(), null)
        assertRequestMatchesEnvelope(full, ComplexRequest.serializer())
        assertRequestMatchesEnvelope(empty, ComplexRequest.serializer())
        assertResponseMatchesEnvelope(ComplexResponse(full.nested, full.items, null), ComplexResponse.serializer())
        assertResponseMatchesEnvelope(ComplexResponse(empty.nested, empty.items, 3), ComplexResponse.serializer())
    }

    @Test
    fun `numeric edge values match envelope encoding`() {
        assertRequestMatchesEnvelope(
            NumericRequest(Long.MIN_VALUE, 1.0 / 3.0, -0.0, Double.MIN_VALUE, Long.MAX_VALUE),
            NumericRequest.serializer()
        )
    }

    @Test
    fun `unicode, escaped text and contextual uuid match envelope encoding`() {
        val text = "Grüße \"quoted\" / \\ \n\t ünïcödé ✨ 𝄞"
        assertRequestMatchesEnvelope(TextRequest(text, UUID.randomUUID()), TextRequest.serializer())
        assertResponseMatchesEnvelope(TextResponse(text), TextResponse.serializer())
    }

    @Test
    fun `random request ids match envelope encoding`() {
        val random = Random(18)
        repeat(500) {
            val id = UUID(random.nextLong(), random.nextLong())
            assertRequestMatchesEnvelope(ScalarRequest(), ScalarRequest.serializer(), id)
            assertResponseMatchesEnvelope(ScalarResponse(), ScalarResponse.serializer(), id)
        }
    }

    @Test
    fun `inbound head scan reads the streaming envelopes`() {
        val id = UUID.randomUUID()
        val request = json.encodeToString(serializers.request(ScalarRequest::class.java)!!, RequestEnvelopeContent(id, ScalarRequest()))
        val response = json.encodeToString(serializers.response(ScalarResponse::class.java)!!, RequestEnvelopeContent(id, ScalarResponse()))

        assertEquals(
            ScalarRequest::class.java.name,
            JsonEnvelopeHead.secondString(
                request,
                RequestResponseBusImpl.REQUEST_ID_PREFIX,
                RequestResponseBusImpl.REQUEST_CLASS_SEPARATOR
            )
        )
        assertEquals(id, JsonEnvelopeHead.leadingUuid(response, RequestResponseBusImpl.REQUEST_ID_PREFIX))
        assertEquals(
            ScalarResponse::class.java.name,
            JsonEnvelopeHead.secondString(
                response,
                RequestResponseBusImpl.REQUEST_ID_PREFIX,
                RequestResponseBusImpl.RESPONSE_CLASS_SEPARATOR
            )
        )
    }

    @Test
    fun `decode tolerates unknown keys and reordered fields`() {
        val id = UUID.randomUUID()
        val request = json.decodeFromString(
            serializers.request(ScalarRequest::class.java)!!,
            "{\"request_data\":{\"enabled\":false,\"unknown\":{},\"count\":3},\"extra\":1," +
                    "\"request_class\":\"${ScalarRequest::class.java.name}\",\"request_id\":\"$id\"}"
        )
        assertEquals(id, request.requestId)
        assertEquals(3, (request.payload as ScalarRequest).count)
        assertEquals(false, (request.payload as ScalarRequest).enabled)

        val response = json.decodeFromString(
            serializers.response(ScalarResponse::class.java)!!,
            "{\"response_class\":\"${ScalarResponse::class.java.name}\",\"response_data\":{\"status\":404}," +
                    "\"request_id\":\"$id\",\"extra\":null}"
        )
        assertEquals(id, response.requestId)
        assertEquals(404, (response.payload as ScalarResponse).status)
    }

    @Test
    fun `decode rejects malformed envelopes with a serialization exception`() {
        val id = UUID.randomUUID()
        val name = ScalarResponse::class.java.name
        val data = "{\"status\":404}"
        val malformed = listOf(
            "{\"response_class\":\"$name\",\"response_data\":$data}",
            "{\"request_id\":\"$id\",\"response_data\":$data}",
            "{\"request_id\":\"$id\",\"response_class\":\"$name\"}",
            "{\"request_id\":\"$id\",\"response_class\":\"${TextResponse::class.java.name}\",\"response_data\":$data}",
            "{\"request_id\":\"$id\",\"response_class\":\"$name\",\"response_data\":{\"status\":true}}",
            "{\"request_id\":\"$id\",\"response_class\":\"$name\",\"response_data\":$data}garbage",
            "{\"request_id\":\"$id\",\"response_class\":\"$name\",\"response_data\":",
        )
        val serializer = serializers.response(ScalarResponse::class.java)!!
        for (message in malformed) {
            assertFailsWith<SerializationException>(message) { json.decodeFromString(serializer, message) }
        }
    }

    @Test
    fun `unserializable payload has no envelope serializer`() {
        assertNull(serializers.request(UnserializableRequest::class.java))
        assertNull(serializers.response(UnserializableResponse::class.java))
    }

    @Test
    fun `envelope serializer is memoized per payload class`() {
        assertSame(serializers.request(ScalarRequest::class.java), serializers.request(ScalarRequest::class.java))
        assertSame(serializers.response(ScalarResponse::class.java), serializers.response(ScalarResponse::class.java))
    }

    @Serializable
    class ScalarRequest(val count: Int = 7, val enabled: Boolean = true) : RedisRequest()

    @Serializable
    class ScalarResponse(val status: Int = 200) : RedisResponse()

    @Serializable
    data class Nested(val label: String, val flag: Boolean)

    @Serializable
    class ComplexRequest(
        val nested: Nested,
        val items: List<String>,
        val lookup: Map<String, Int>,
        val nullableNested: Nested?
    ) : RedisRequest()

    @Serializable
    class ComplexResponse(val nested: Nested, val items: List<String>, val optional: Int?) : RedisResponse()

    @Serializable
    class NumericRequest(
        val whole: Long,
        val fraction: Double,
        val negativeZero: Double,
        val tiny: Double,
        val big: Long
    ) : RedisRequest()

    @Serializable
    class TextRequest(val text: String, @Contextual val target: UUID) : RedisRequest()

    @Serializable
    class TextResponse(val text: String) : RedisResponse()

    class UnserializableRequest : RedisRequest()

    class UnserializableResponse : RedisResponse()
}
