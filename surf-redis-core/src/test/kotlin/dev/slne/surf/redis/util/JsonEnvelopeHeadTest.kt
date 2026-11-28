package dev.slne.surf.redis.util

import dev.slne.surf.api.core.serializer.java.uuid.JavaUUIDStringSerializer
import dev.slne.surf.redis.event.EventEnvelopeSerializers
import dev.slne.surf.redis.event.RedisEvent
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNamingStrategy
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.contextual
import java.util.*
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/**
 * Pins that [JsonEnvelopeHead] either returns exactly the value a full decode yields or `null`, for the
 * envelope shapes the event and request/response buses put on the wire. The request/response mirrors
 * annotate `request_id` with [JavaUUIDStringSerializer] exactly as the product envelopes do.
 */
class JsonEnvelopeHeadTest {

    /** Mirrors `RedisApi.createJson`. */
    @OptIn(ExperimentalSerializationApi::class)
    private val json = Json {
        ignoreUnknownKeys = true
        namingStrategy = JsonNamingStrategy.SnakeCase
        encodeDefaults = true
        serializersModule = SerializersModule { contextual(JavaUUIDStringSerializer) }
    }

    private val eventPrefix = JsonEnvelopeHead.prefix("event_class")
    private val requestIdPrefix = JsonEnvelopeHead.prefix("request_id")
    private val requestClassSeparator = "\",\"request_class\":\""

    @Test
    fun `event class is read from the streaming event envelope`() {
        val message = json.encodeToString(
            EventEnvelopeSerializers(json.serializersModule).get(HeadEvent::class.java)!!,
            HeadEvent(payload = "x\"y\\z", nested = listOf("a", "b"))
        )
        assertEquals(HeadEvent::class.java.name, JsonEnvelopeHead.leadingString(message, eventPrefix))
    }

    @Test
    fun `leading string matches a full decode or is null for arbitrary values`() {
        val random = Random(17)
        val alphabet = "abcXYZ019.\$_-äß€😀\"\\/\n\t\u0001 "
        val values = listOf("", "a", "\"", "\\", "a\"b", "a\\b", "\u0000", "€", "\uD83D") +
            List(2_000) { String(CharArray(random.nextInt(0, 24)) { alphabet[random.nextInt(alphabet.length)] }) }

        for (value in values) {
            val message = json.encodeToString(EventEnvelope(value, JsonPrimitive("data")))
            val head = JsonEnvelopeHead.leadingString(message, eventPrefix) ?: continue
            assertEquals(json.decodeFromString<EventEnvelope>(message).eventClass, head, message)
        }
    }

    @Test
    fun `values containing an escape fall back`() {
        val message = json.encodeToString(EventEnvelope("a\"b", JsonPrimitive(1)))
        assertNull(JsonEnvelopeHead.leadingString(message, eventPrefix))
    }

    @Test
    fun `unrecognized shapes fall back`() {
        val cases = listOf(
            """{"event_data":{},"event_class":"a.B"}""",
            """{ "event_class":"a.B","event_data":{}}""",
            """{"event_class": "a.B","event_data":{}}""",
            """{"eventClass":"a.B","eventData":{}}""",
            """{"event_class":1,"event_data":{}}""",
            """{"event_class":"a.B""",
            """{"event_class":""",
            "",
        )
        for (message in cases) {
            assertNull(JsonEnvelopeHead.leadingString(message, eventPrefix), message)
        }
    }

    @Test
    fun `an intact head of a truncated message is still returned`() {
        // Routing may use it; the full decode on the handling instance rejects the message as before.
        assertEquals("a.B", JsonEnvelopeHead.leadingString("{\"event_class\":\"a.B\"", eventPrefix))
        assertEquals("", JsonEnvelopeHead.leadingString("{\"event_class\":\"\"", eventPrefix))
    }

    @Test
    fun `request class and id are read from the request envelope`() {
        val random = Random(23)
        val ids = listOf(UUID(0, 0), UUID(-1, -1), UUID(Long.MIN_VALUE, Long.MAX_VALUE)) +
            List(500) { UUID(random.nextLong(), random.nextLong()) }
        for (id in ids) {
            val message = json.encodeToString(RequestEnvelope(id, "com.example.Ping", JsonPrimitive("x")))
            assertEquals(
                "com.example.Ping",
                JsonEnvelopeHead.secondString(message, requestIdPrefix, requestClassSeparator),
                message
            )
            assertEquals(id, JsonEnvelopeHead.leadingUuid(message, requestIdPrefix), message)
        }
    }

    @Test
    fun `response id matches the product UUID serializer`() {
        val random = Random(29)
        repeat(500) {
            val id = UUID(random.nextLong(), random.nextLong())
            val message = json.encodeToString(ResponseEnvelope(id, "a.B", JsonPrimitive(1)))
            val decoded = json.decodeFromString<ResponseEnvelope>(message).requestId
            assertEquals(decoded, JsonEnvelopeHead.leadingUuid(message, requestIdPrefix), message)
        }
        val id = UUID.randomUUID()
        val upper = """{"request_id":"${id.toString().uppercase()}","response_class":"a.B","response_data":1}"""
        assertEquals(json.decodeFromString<ResponseEnvelope>(upper).requestId, JsonEnvelopeHead.leadingUuid(upper, requestIdPrefix))
    }

    @Test
    fun `non-canonical request ids fall back`() {
        val id = UUID.randomUUID()
        val cases = listOf(
            """{"request_id":"${id.toString().replace("-", "")}","request_class":"a.B","request_data":1}""",
            """{"request_id":"1-1-1-1-1","request_class":"a.B","request_data":1}""",
            """{"request_id":"zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz","request_class":"a.B","request_data":1}""",
            """{"request_id":[${List(16) { 0 }.joinToString(",")}],"request_class":"a.B","request_data":1}""",
            """{"request_class":"a.B","request_id":"$id","request_data":1}""",
            """{"request_id":"$id""",
        )
        for (message in cases) {
            assertNull(JsonEnvelopeHead.leadingUuid(message, requestIdPrefix), message)
        }
    }

    @Test
    fun `request class falls back when the envelope shape differs`() {
        val id = UUID.randomUUID()
        val cases = listOf(
            """{"request_class":"a.B","request_id":"$id","request_data":1}""",
            """{"request_id":"$id","request_data":1,"request_class":"a.B"}""",
            """{"request_id":"x\"y","request_class":"a.B","request_data":1}""",
            """{"request_id":"$id","request_class":"a\"B","request_data":1}""",
            """{"request_id":"$id", "request_class":"a.B","request_data":1}""",
            """{"request_id":"$id","request_class":"a.B""",
            """{"request_id":"$id"""",
            """{"request_id":[${List(16) { 0 }.joinToString(",")}],"request_class":"a.B","request_data":1}""",
        )
        for (message in cases) {
            assertNull(JsonEnvelopeHead.secondString(message, requestIdPrefix, requestClassSeparator), message)
        }
    }

    @Serializable
    data class HeadEvent(val payload: String, val nested: List<String>) : RedisEvent()

    @Serializable
    private class EventEnvelope(val eventClass: String, val eventData: JsonElement)

    @Serializable
    private class RequestEnvelope(
        @Serializable(with = JavaUUIDStringSerializer::class) val requestId: UUID,
        val requestClass: String,
        val requestData: JsonElement
    )

    @Serializable
    private class ResponseEnvelope(
        @Serializable(with = JavaUUIDStringSerializer::class) val requestId: UUID,
        val responseClass: String,
        val responseData: JsonElement
    )
}
