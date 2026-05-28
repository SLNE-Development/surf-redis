@file:Suppress("InternalApiUsage")

package dev.slne.surf.redis.request

import com.google.common.flogger.StackSize
import dev.slne.surf.api.core.invoker.HiddenInvokerUtil
import dev.slne.surf.api.core.invoker.InvokerFactory
import dev.slne.surf.api.core.serializer.java.uuid.SerializableUUID
import dev.slne.surf.api.core.util.logger
import dev.slne.surf.api.shared.api.util.InternalInvokerApi
import dev.slne.surf.redis.RedisApi
import dev.slne.surf.redis.RedisComponentProvider
import dev.slne.surf.redis.invoker.RedisInvokerLookupProvider
import dev.slne.surf.redis.invoker.RedisRequestHandlerInvokerTemplate
import dev.slne.surf.redis.util.KotlinSerializerCache
import dev.slne.surf.redis.util.asDeferred
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import kotlinx.coroutines.*
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.descriptors.element
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.encoding.encodeStructure
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.serializer
import reactor.core.Disposable
import reactor.core.publisher.Mono
import java.lang.reflect.ParameterizedType
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.write
import kotlin.time.Duration.Companion.milliseconds
import org.redisson.client.codec.StringCodec.INSTANCE as StringCodec

@OptIn(InternalInvokerApi::class)
@Suppress("UnstableApiUsage")
class RequestResponseBusImpl(private val api: RedisApi) : RequestResponseBus {

    /**
     * Registered request handlers indexed by the concrete [RedisRequest] subclass they handle.
     *
     * Each entry maps a request type to a single [RedisRequestHandlerInvoker] instance.
     * Invokers are hidden-class-backed wrappers around the original handler `MethodHandle`,
     * enabling JIT-constant-folding of the dispatch target.
     *
     * Only one handler per request type is allowed; duplicate registrations are logged and ignored.
     */
    private val requestHandlers =
        Object2ObjectOpenHashMap<Class<out RedisRequest>, RedisRequestHandlerInvoker>()
    private val pendingRequests = ConcurrentHashMap<UUID, CompletableDeferred<RedisResponse>>()

    private val requestTypeRegistry = Object2ObjectOpenHashMap<String, Class<out RedisRequest>>()
    private val responseTypeRegistry = ConcurrentHashMap<String, Class<out RedisResponse>>()

    /**
     * Read-write lock guarding mutations to [requestHandlers] during handler registration.
     *
     * After the owning [RedisApi] is frozen, no further registration is allowed and
     * request dispatching operates as a read-only path, so this lock is not contended
     * during normal operation.
     */
    private val registrationLock = ReentrantReadWriteLock()

    private val serializerCache = KotlinSerializerCache<Any>(api.json.serializersModule)

    private val requestTopic by lazy { api.redissonReactive.getTopic(REQUEST_CHANNEL, StringCodec) }
    private val responseTopic by lazy {
        api.redissonReactive.getTopic(
            RESPONSE_CHANNEL,
            StringCodec
        )
    }

    private lateinit var requestDisposable: Disposable
    private lateinit var responseDisposable: Disposable

    companion object {
        private val log = logger()
        private const val REQUEST_CHANNEL = "surf-redis:requests"
        private const val RESPONSE_CHANNEL = "surf-redis:responses"

        private val INVOKER_FACTORY = InvokerFactory(
            /* templateClass = */ RedisRequestHandlerInvokerTemplate::class.java,
            /* invokerInterface = */ RedisRequestHandlerInvoker::class.java,
            /* lookup = */ RedisInvokerLookupProvider.LOOKUP
        )
    }

    /**
     * Initializes the bus by subscribing to request/response channels.
     *
     * This method is blocking and should only be called during startup.
     */
    override fun init(): Mono<Void> = Mono.fromRunnable { setupSubscription() }

    /**
     * Installs the Redis Pub/Sub listener and subscribes to request/response channels.
     *
     * Incoming messages are dispatched to handler coroutines on `Dispatchers.Default`.
     */
    private fun setupSubscription() {
        val requestId =
            requestTopic.addListener(String::class.java) { _, msg -> handleIncomingRequest(msg) }
                .block()
        val responseId =
            responseTopic.addListener(String::class.java) { _, msg -> handleIncomingResponse(msg) }
                .block()

        requestDisposable = {
            requestTopic.removeListener(requestId).block()
        }
        responseDisposable = {
            responseTopic.removeListener(responseId).block()
        }
    }

    /**
     * Handles an incoming request message.
     *
     * The request envelope is decoded, the request is deserialized, and the registered handler for the
     * request type is invoked. The provided [RequestContext] exposes a response callback which publishes
     * a response back to Redis.
     */
    private fun handleIncomingRequest(message: String) {
        val envelope = try {
            api.json.decodeFromString<RequestEnvelope>(message)
        } catch (e: SerializationException) {
            log.atFine()
                .withCause(e)
                .log("Unable to deserialize request envelope: ${e.message}")
            return
        }

        val requestClass = requestTypeRegistry[envelope.requestClass]

        if (requestClass == null) {
            log.atFine()
                .log("No registered request class for name: ${envelope.requestClass} - ignoring request.")
            return
        }

        val request = deserializeRequest(requestClass, envelope.requestData) ?: return
        val handler = requestHandlers[requestClass] ?: return
        val context = RequestContext(
            request = request,
            respondCallback = { response ->
                sendResponse(envelope.requestId, response)
            },
            coroutineScope = api.redisListenerScope
        )

        api.redisListenerScope.launch {
            try {
                handler.invoke(context)
            } catch (e: Throwable) {
                if (e is CancellationException) throw e
                log.atWarning()
                    .withCause(e)
                    .log("Error handling request ${request::class.simpleName}: ${e.message}")
            }
        }
    }

    /**
     * Handles an incoming response message.
     *
     * The response envelope is decoded and the corresponding pending request is completed.
     * Responses without a matching pending request are ignored.
     */
    private fun handleIncomingResponse(message: String) {
        val envelope = try {
            api.json.decodeFromString<ResponseEnvelope>(message)
        } catch (e: SerializationException) {
            log.atFine()
                .withCause(e)
                .log("Unable to deserialize response envelope: ${e.message}")
            return
        }

        val responseClass = responseTypeRegistry[envelope.responseClass]

        if (responseClass == null) {
            log.atFine()
                .log("No registered response class for name: ${envelope.responseClass} - ignoring response.")
            return
        }

        val response = deserializeResponse(responseClass, envelope.responseData) ?: return
        val deferred = pendingRequests.remove(envelope.requestId)
        if (deferred == null) {
            log.atFine()
                .log("No pending request found for response with ID: ${envelope.requestId} - ignoring response.")
            return
        }

        deferred.complete(response)
    }

    override suspend fun <T : RedisResponse> sendRequest(
        request: RedisRequest,
        responseType: Class<T>,
        timeoutMs: Long
    ): T {
        RedisComponentProvider.injectOriginId(request)

        @Suppress("UNCHECKED_CAST")
        val serializer = (serializerCache.get(request.javaClass)
            ?: throw IllegalStateException("No serializer found for request class: ${request::class.simpleName}"))
                as KSerializer<RedisRequest>

        val requestId = UUID.randomUUID()
        val deferred = CompletableDeferred<RedisResponse>()

        pendingRequests[requestId] = deferred
        // Avoid a String.hashCode + ConcurrentHashMap put on every request when this response
        // type has already been registered. putIfAbsent is atomic, so a redundant put from a
        // concurrent thread is harmless.
        if (!responseTypeRegistry.containsKey(responseType.name)) {
            responseTypeRegistry.putIfAbsent(responseType.name, responseType)
        }

        val message = try {
            api.json.encodeToString(
                RequestEnvelopeSerializer(serializer),
                RequestEnvelopePayload(requestId, request.javaClass.name, request)
            )
        } catch (e: SerializationException) {
            pendingRequests.remove(requestId)
            log.atWarning()
                .withCause(e)
                .log("Unable to serialize request: ${e.message}")
            throw IllegalStateException("Unable to serialize request: ${request::class.simpleName}", e)
        }

        requestTopic.publish(message).awaitSingle()

        try {
            return withTimeout(timeoutMs.milliseconds) {
                val response = deferred.await()
                responseType.cast(response)
            }
        } catch (_: TimeoutCancellationException) {
            pendingRequests.remove(requestId)
            throw RequestTimeoutException("Request timed out after ${timeoutMs}ms: ${request::class.simpleName}")
        } catch (e: Exception) {
            pendingRequests.remove(requestId)
            throw e
        }
    }

    /**
     * Publishes a response for a given request ID.
     *
     * @return a [kotlinx.coroutines.Deferred] containing the number of receiving subscribers,
     *         or `0` if the response could not be serialized
     */
    private fun sendResponse(requestId: UUID, response: RedisResponse): Deferred<Long> {
        val rawSerializer = serializerCache.get(response.javaClass)
        if (rawSerializer == null) {
            log.atWarning()
                .log("No serializer found for response class: ${response::class.simpleName} - ignoring response.")
            return CompletableDeferred(0L)
        }

        @Suppress("UNCHECKED_CAST")
        val serializer = rawSerializer as KSerializer<RedisResponse>

        val message = try {
            api.json.encodeToString(
                ResponseEnvelopeSerializer(serializer),
                ResponseEnvelopePayload(requestId, response.javaClass.name, response)
            )
        } catch (e: SerializationException) {
            log.atWarning()
                .withCause(e)
                .log("Unable to serialize response: ${e.message}")
            return CompletableDeferred(0L)
        }

        return responseTopic.publish(message).asDeferred()
    }

    override fun registerRequestHandler(handler: Any) {
        require(!api.isFrozen()) { "Cannot register request handler after RedisApi has been frozen." }

        val handlerClass = handler.javaClass
        val methods = handlerClass.declaredMethods

        for (method in methods) {
            if (!method.isAnnotationPresent(HandleRedisRequest::class.java)) continue
            val validParamCount = when {
                HiddenInvokerUtil.isSuspendFunction(method) -> 2
                else -> 1
            }

            if (method.parameterCount != validParamCount) {
                log.atWarning()
                    .withStackTrace(StackSize.MEDIUM)
                    .log("Method ${method.name} in class ${handlerClass.name} is annotated with @RequestHandler but has ${method.parameterCount} parameters. Expected exactly 1 parameter of type RequestContext<T>.")
                continue
            }

            val paramRaw = method.parameterTypes[0]
            if (!RequestContext::class.java.isAssignableFrom(paramRaw)) {
                log.atWarning()
                    .withStackTrace(StackSize.MEDIUM)
                    .log("Method ${method.name} in class ${handlerClass.name} is annotated with @RequestHandler but has parameter of type ${paramRaw.name}. Expected parameter of type RequestContext<T>.")
                continue
            }

            val generic = method.genericParameterTypes[0]
            if (generic !is ParameterizedType) {
                log.atWarning()
                    .withStackTrace(StackSize.MEDIUM)
                    .log("Method ${method.name} in class ${handlerClass.name} has non-parameterized RequestContext parameter.")
                continue
            }

            val requestType = generic.actualTypeArguments[0] as? Class<*>

            if (requestType == null) {
                log.atWarning()
                    .withStackTrace(StackSize.MEDIUM)
                    .log("Method ${method.name} in class ${handlerClass.name} is annotated with @RequestHandler but has parameter of type RequestContext<T> but T is not a class.")
                continue
            }

            if (!RedisRequest::class.java.isAssignableFrom(requestType)) {
                log.atWarning()
                    .withStackTrace(StackSize.MEDIUM)
                    .log("Method ${method.name} in class ${handlerClass.name} is annotated with @RequestHandler but has parameter of type RequestContext<${requestType.name}>. Expected parameter of type RequestContext<T> where T is a subclass of RedisRequest.")
                continue
            }

            if (!INVOKER_FACTORY.canAccess(handler, method)) {
                log.atSevere()
                    .withStackTrace(StackSize.MEDIUM)
                    .log(
                        "Method ${method.name} in ${handlerClass.name} is not accessible via privateLookupIn " +
                                "— ensure the package '${handlerClass.packageName}' is opened to the surf-redis module. " +
                                "Cannot register as request handler."
                    )
                continue
            }

            @Suppress("UNCHECKED_CAST")
            requestType as Class<out RedisRequest>

            val invoker = INVOKER_FACTORY.create(handler, method, requestType)
            val current = registrationLock.write {
                requestTypeRegistry[requestType.name] = requestType
                requestHandlers.putIfAbsent(requestType, invoker)
            }

            if (current != null) {
                log.atWarning()
                    .withStackTrace(StackSize.MEDIUM)
                    .log("A request handler is already registered for request type: ${requestType.name} - ignoring duplicate registration in method ${method.name} of class ${handlerClass.name}.")
            }
        }
    }

    /**
     * Deserializes a request of the given type from JSON.
     *
     * @return the deserialized request, or `null` if no serializer is available or deserialization fails
     */
    private fun deserializeRequest(
        requestClass: Class<out RedisRequest>,
        requestData: JsonElement
    ): RedisRequest? {
        val serializer = serializerCache.get(requestClass)

        if (serializer == null) {
            log.atWarning()
                .log("No serializer found for request class: ${requestClass.name}")
            return null
        }

        try {
            val deserialized = api.json.decodeFromJsonElement(serializer, requestData)
            if (deserialized !is RedisRequest) {
                log.atWarning()
                    .log("Deserialized object is not a RedisRequest: ${deserialized::class.simpleName}")
                return null
            }

            return deserialized
        } catch (e: SerializationException) {
            log.atWarning()
                .withCause(e)
                .log("Unable to deserialize request: ${e.message}")
            return null
        }
    }

    /**
     * Deserializes a response of the given type from JSON.
     *
     * @return the deserialized response, or `null` if no serializer is available or deserialization fails
     */
    private fun deserializeResponse(
        responseClass: Class<out RedisResponse>,
        responseData: JsonElement
    ): RedisResponse? {
        val serializer = serializerCache.get(responseClass)

        if (serializer == null) {
            log.atWarning()
                .log("No serializer found for response class: ${responseClass.name} - ignoring response.")
            return null
        }

        try {
            val deserialized = api.json.decodeFromJsonElement(serializer, responseData)
            if (deserialized !is RedisResponse) {
                log.atWarning()
                    .log("Deserialized object is not a RedisResponse: ${deserialized::class.simpleName}")
                return null
            }

            return deserialized
        } catch (e: SerializationException) {
            log.atWarning()
                .withCause(e)
                .log("Unable to deserialize response: ${e.message}")
            return null
        }
    }

    /**
     * Cancels all pending requests and clears internal state.
     */
    override fun close() {
        requestDisposable.dispose()
        responseDisposable.dispose()

        pendingRequests.values.forEach { deferred ->
            deferred.cancel("RequestResponseBus closed")
        }
        pendingRequests.clear()

        requestHandlers.clear()
        requestTypeRegistry.clear()
        responseTypeRegistry.clear()
    }

    /**
     * Serializer used to encode the [SerializableUUID] type alias for the request id wire field.
     *
     * Resolved once and reused across both envelope serializers.
     */
    private val uuidSerializer: KSerializer<SerializableUUID> =
        api.json.serializersModule.serializer()

    /**
     * Wire format for request messages published to Redis.
     *
     * Used only for **decoding** incoming messages. For encoding, see [RequestEnvelopeSerializer].
     */
    @Serializable
    private data class RequestEnvelope(
        val requestId: SerializableUUID,
        val requestClass: String,
        val requestData: JsonElement
    )

    /**
     * Wire format for response messages published to Redis.
     *
     * Used only for **decoding** incoming messages. For encoding, see [ResponseEnvelopeSerializer].
     */
    @Serializable
    private data class ResponseEnvelope(
        val requestId: SerializableUUID,
        val responseClass: String,
        val responseData: JsonElement
    )

    /**
     * Encode-only payload pairing a request with its routing metadata.
     */
    private data class RequestEnvelopePayload(
        val requestId: UUID,
        val requestClass: String,
        val request: RedisRequest
    )

    /**
     * Encode-only payload pairing a response with its routing metadata.
     */
    private data class ResponseEnvelopePayload(
        val requestId: UUID,
        val responseClass: String,
        val response: RedisResponse
    )

    /**
     * Streaming JSON serializer for outgoing request envelopes.
     *
     * Delegates payload encoding directly to the typed [dataSerializer], avoiding the
     * intermediate [JsonElement] tree allocation that the symmetric data-class based path
     * would incur. Element names match [RequestEnvelope] so the configured
     * [kotlinx.serialization.json.JsonNamingStrategy] yields an identical wire format.
     */
    @OptIn(ExperimentalSerializationApi::class)
    private inner class RequestEnvelopeSerializer(
        private val dataSerializer: KSerializer<RedisRequest>
    ) : KSerializer<RequestEnvelopePayload> {
        override val descriptor: SerialDescriptor =
            buildClassSerialDescriptor("RequestEnvelope") {
                element("requestId", uuidSerializer.descriptor)
                element<String>("requestClass")
                element("requestData", dataSerializer.descriptor)
            }

        override fun serialize(encoder: Encoder, value: RequestEnvelopePayload) {
            encoder.encodeStructure(descriptor) {
                encodeSerializableElement(descriptor, 0, uuidSerializer, value.requestId)
                encodeStringElement(descriptor, 1, value.requestClass)
                encodeSerializableElement(descriptor, 2, dataSerializer, value.request)
            }
        }

        override fun deserialize(decoder: Decoder): RequestEnvelopePayload =
            error("RequestEnvelopeSerializer is encode-only")
    }

    /**
     * Streaming JSON serializer for outgoing response envelopes. See [RequestEnvelopeSerializer].
     */
    @OptIn(ExperimentalSerializationApi::class)
    private inner class ResponseEnvelopeSerializer(
        private val dataSerializer: KSerializer<RedisResponse>
    ) : KSerializer<ResponseEnvelopePayload> {
        override val descriptor: SerialDescriptor =
            buildClassSerialDescriptor("ResponseEnvelope") {
                element("requestId", uuidSerializer.descriptor)
                element<String>("responseClass")
                element("responseData", dataSerializer.descriptor)
            }

        override fun serialize(encoder: Encoder, value: ResponseEnvelopePayload) {
            encoder.encodeStructure(descriptor) {
                encodeSerializableElement(descriptor, 0, uuidSerializer, value.requestId)
                encodeStringElement(descriptor, 1, value.responseClass)
                encodeSerializableElement(descriptor, 2, dataSerializer, value.response)
            }
        }

        override fun deserialize(decoder: Decoder): ResponseEnvelopePayload =
            error("ResponseEnvelopeSerializer is encode-only")
    }
}