package dev.slne.surf.redis.event

import dev.slne.surf.api.core.util.emptyObject2ObjectMap
import dev.slne.surf.api.core.util.freeze
import dev.slne.surf.redis.codec.RedisCodec
import dev.slne.surf.redis.codec.writeString
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufUtil
import io.netty.buffer.Unpooled
import it.unimi.dsi.fastutil.objects.Object2ObjectMap
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import it.unimi.dsi.fastutil.objects.ObjectArrayList
import java.lang.reflect.Field
import java.lang.reflect.Modifier
import java.nio.charset.StandardCharsets

internal class EventCodecRegistry {
    typealias ByClass = Object2ObjectMap<Class<out RedisEvent>, EventCodecRegistration>
    typealias ById = Object2ObjectMap<String, EventCodecRegistration>

    private val registrationsByClass: ByClass = Object2ObjectOpenHashMap()
    private val registrationsById: ById = Object2ObjectOpenHashMap()

    @Volatile
    private var frozenByClass: ByClass = emptyObject2ObjectMap()

    @Volatile
    private var frozenById: ById = emptyObject2ObjectMap()

    @Volatile
    private var frozen = false

    private val discoveredCodecs = object : ClassValue<RedisEventCodec<out RedisEvent>?>() {
        override fun computeValue(type: Class<*>): RedisEventCodec<out RedisEvent>? {
            if (!RedisEvent::class.java.isAssignableFrom(type)) return null
            return discoverCompanionCodec(type)
        }
    }

    private val onDemandRegistrations = object : ClassValue<EventCodecRegistration?>() {
        override fun computeValue(type: Class<*>): EventCodecRegistration? {
            val codec = discoveredCodecs.get(type) ?: return null

            @Suppress("UNCHECKED_CAST")
            return registrationUnchecked(type as Class<out RedisEvent>, codec, explicit = false)
        }
    }

    @Synchronized
    fun <E : RedisEvent> registerExplicit(eventType: Class<E>, codec: RedisEventCodec<E>) {
        check(!frozen) { "Cannot register an event codec after RedisApi has been frozen" }

        val registration = registration(eventType, codec, explicit = true)
        val previous = registrationsByClass[eventType]
        if (previous?.explicit == true) {
            throw IllegalArgumentException("An explicit codec is already registered for event '${eventType.name}'")
        }

        val collision = registrationsById[registration.eventId]
        if (collision != null && collision.eventType != eventType) {
            throw eventIdCollision(registration, collision)
        }

        if (previous != null) registrationsById.remove(previous.eventId, previous)
        registrationsByClass[eventType] = registration
        registrationsById[registration.eventId] = registration
    }

    @Synchronized
    fun registerDiscovered(eventType: Class<out RedisEvent>) {
        check(!frozen) { "Cannot register an event type after RedisApi has been frozen" }

        if (registrationsByClass.containsKey(eventType)) return

        val codec = discoveredCodecs.get(eventType) ?: return
        val registration = registrationUnchecked(eventType, codec, explicit = false)

        val collision = registrationsById[registration.eventId]
        if (collision != null && collision.eventType != eventType) {
            throw eventIdCollision(registration, collision)
        }

        registrationsByClass[eventType] = registration
        registrationsById[registration.eventId] = registration
    }

    @Synchronized
    fun freeze() {
        check(!frozen) { "Event codec registry is already frozen" }
        frozenByClass = Object2ObjectOpenHashMap(registrationsByClass).freeze()
        frozenById = Object2ObjectOpenHashMap(registrationsById).freeze()
        frozen = true
    }

    /**
     * Resolves the codec registration for publishing events of the specified type.
     *
     * This method retrieves a previously registered or discovered codec for the given event type.
     * If no codec has been registered or discovered for the provided type, it returns `null`.
     * If a codec is found, but its associated event ID conflicts with one already registered for a
     * different event type, an exception is thrown to signal the collision.
     *
     * @param eventType The class type of the event for which the codec registration is requested.
     *                  Must extend [RedisEvent].
     * @return The [EventCodecRegistration] for the specified event type if a codec is found;
     *         otherwise, `null`.
     * @throws IllegalArgumentException If an event ID collision is detected during the lookup process.
     */
    fun codecForPublishing(eventType: Class<out RedisEvent>): EventCodecRegistration? {
        val registered = if (frozen) {
            frozenByClass[eventType]
        } else synchronized(this) {
            registrationsByClass[eventType]
        }

        if (registered != null) return registered

        val registration = onDemandRegistrations.get(eventType) ?: return null
        val collision = if (frozen) {
            frozenById[registration.eventId]
        } else synchronized(this) {
            registrationsById[registration.eventId]
        }

        if (collision != null && collision.eventType != eventType) {
            throw eventIdCollision(registration, collision)
        }
        return registration
    }

    /**
     * Retrieves the codec registration associated with the specified event ID.
     *
     * This method attempts to retrieve the appropriate [EventCodecRegistration] based on the given
     * event ID. If the registry is in a frozen state, it checks the frozen mappings; otherwise, it
     * performs a thread-safe lookup in the active registry.
     *
     * @param eventId The unique identifier of the event for which the codec is requested.
     * @return The [EventCodecRegistration] corresponding to the specified event ID if it exists;
     *         otherwise, `null`.
     */
    fun codecForEventId(eventId: String): EventCodecRegistration? {
        return if (frozen) {
            frozenById[eventId]
        } else synchronized(this) {
            registrationsById[eventId]
        }
    }

    @Synchronized
    fun clear() {
        for (eventType in registrationsByClass.keys) {
            discoveredCodecs.remove(eventType)
            onDemandRegistrations.remove(eventType)
        }
        registrationsByClass.clear()
        registrationsById.clear()
        frozenByClass = emptyObject2ObjectMap()
        frozenById = emptyObject2ObjectMap()
    }

    private fun <E : RedisEvent> registration(
        eventType: Class<E>,
        codec: RedisEventCodec<E>,
        explicit: Boolean
    ): EventCodecRegistration = registrationUnchecked(eventType, codec, explicit)

    private fun registrationUnchecked(
        eventType: Class<out RedisEvent>,
        codec: RedisEventCodec<out RedisEvent>,
        explicit: Boolean
    ): EventCodecRegistration {
        val eventId = codec.eventId
        require(eventId.isNotBlank()) { "Event codec ID for '${eventType.name}' must not be blank" }
        val encodedLength = eventId.toByteArray(StandardCharsets.UTF_8).size
        require(encodedLength <= MAX_EVENT_ID_BYTES) {
            "Event codec ID for '${eventType.name}' is $encodedLength bytes; maximum is $MAX_EVENT_ID_BYTES"
        }
        validateCodecIdentity(codec, eventType)

        @Suppress("UNCHECKED_CAST")
        return EventCodecRegistration(
            eventType,
            codec as RedisEventCodec<RedisEvent>,
            eventId,
            codec.version,
            explicit
        )
    }

    private fun validateCodecIdentity(codec: RedisCodec<*>, eventType: Class<out RedisEvent>) {
        require(codec.codecId.isNotBlank()) { "Codec ID for event '${eventType.name}' must not be blank" }
        require(codec.version > 0) { "Codec version for event '${eventType.name}' must be positive" }
    }

    private fun discoverCompanionCodec(type: Class<*>): RedisEventCodec<out RedisEvent>? {
        val companionField = companionField(type) ?: return null

        val companion = try {
            companionField.get(null)
        } catch (exception: Exception) {
            throw IllegalStateException(
                "Could not access companion object '${companionField.type.name}' " +
                        "of class '${type.name}'",
                exception
            )
        } ?: throw IllegalStateException(
            "Companion object '${companionField.type.name}' of class '${type.name}' " +
                    "has no accessible instance"
        )

        if (companion is RedisEventCodec<*>) return companion
        check(companion !is RedisCodec<*>) { "Companion object for event '${type.name}' implements RedisCodec but not RedisEventCodec" }

        val candidates = ObjectArrayList<RedisEventCodec<out RedisEvent>>(2)
        for (method in companion.javaClass.methods) {
            if (method.parameterCount != 0 || method.name !in CODEC_PROVIDER_METHODS ||
                !RedisEventCodec::class.java.isAssignableFrom(method.returnType)
            ) continue

            val value = method.invoke(companion) ?: continue
            @Suppress("UNCHECKED_CAST")
            candidates += value as RedisEventCodec<out RedisEvent>
        }

        val distinct = candidates.distinctBy { System.identityHashCode(it) }
        require(distinct.size <= 1) {
            "Companion object for event '${type.name}' provides multiple RedisEventCodec instances"
        }
        return distinct.firstOrNull()
    }

    private fun companionField(type: Class<*>): Field? {
        if (!type.isAnnotationPresent(Metadata::class.java)) {
            return null
        }

        return type.declaredFields.find { field ->
            val companionClass = field.type

            if (!Modifier.isStatic(field.modifiers) ||
                !Modifier.isFinal(field.modifiers) ||
                companionClass.declaringClass != type ||
                companionClass.simpleName != field.name
            ) {
                return@find false
            }

            val constructors = companionClass.declaredConstructors
            constructors.all {
                Modifier.isPrivate(it.modifiers) || it.isSynthetic
            } && constructors.any {
                it.isSynthetic
            }
        }
    }

    private fun eventIdCollision(
        attempted: EventCodecRegistration,
        existing: EventCodecRegistration
    ) = IllegalArgumentException(
        "Event codec ID '${attempted.eventId}' for '${attempted.eventType.name}' is already used by '${existing.eventType.name}'"
    )

    companion object {
        const val MAX_EVENT_ID_BYTES = 512
        private val CODEC_PROVIDER_METHODS = setOf("getCodec", "getRedisEventCodec")
    }
}

/**
 * Represents the registration details of a codec for a specific [RedisEvent] type.
 *
 * This class associates a concrete [RedisEvent] type with its corresponding [RedisEventCodec]
 * implementation, uniquely identified by an event ID and version. It also tracks whether the
 * registration is explicitly defined and maintains an estimated packet size for efficient
 * transport of serialized events.
 *
 * @property eventType The class type of the [RedisEvent] associated with this registration.
 * @property codec The codec responsible for encoding and decoding the event.
 * @property eventId A stable identifier for the event, used for routing purposes.
 * @property version The version of the event type or codec implementation.
 * @property explicit Indicates if this registration was explicitly declared by the user.
 */
internal data class EventCodecRegistration(
    val eventType: Class<out RedisEvent>,
    val codec: RedisEventCodec<RedisEvent>,
    val eventId: String,
    val version: Int,
    val explicit: Boolean
) {
    private val encodedEventId: ByteArray = encodeEventId(eventId)

    @Volatile
    private var measuredPacketSizeEstimate: Int = 0

    private var decayGrace: Int = 0

    val packetSizeEstimate: Int
        get() = measuredPacketSizeEstimate.takeIf { it > 0 }
            ?: CustomEventPacketCodec.DEFAULT_INITIAL_CAPACITY

    fun writeEventId(buffer: ByteBuf) {
        buffer.writeBytes(encodedEventId)
    }

    /**
     * Updates the packet-size estimate used as the initial capacity for future outbound packet buffers.
     *
     * [actualSize] is clamped to the valid packet-size range before being recorded. An observation that
     * is greater than or equal to the current estimate replaces it immediately and resets the decay
     * grace period.
     *
     * Smaller observations leave the estimate unchanged for [PACKET_SIZE_DECAY_GRACE] updates. Once
     * that grace period has elapsed, each smaller observation reduces the estimate by
     * `estimate / 2^[PACKET_SIZE_DECAY_SHIFT]`, but never below the observed size.
     *
     * The estimate is only an allocation hint and does not affect the encoded packet contents.
     *
     * @param actualSize the encoded packet size in bytes
     */
    fun recordPacketSize(actualSize: Int) {
        val observed = actualSize.coerceIn(1, CustomEventPacketCodec.MAX_PACKET_SIZE)
        val previous = measuredPacketSizeEstimate

        if (observed >= previous) {
            measuredPacketSizeEstimate = observed
            decayGrace = PACKET_SIZE_DECAY_GRACE
            return
        }

        val grace = decayGrace
        if (grace > 0) {
            decayGrace = grace - 1
            return
        }

        measuredPacketSizeEstimate = maxOf(observed, previous - (previous shr PACKET_SIZE_DECAY_SHIFT))
    }

    private companion object {
        /**
         * Number of smaller packet observations ignored after the estimate is established or increased
         * before decay is allowed to begin.
         */
        const val PACKET_SIZE_DECAY_GRACE = 1

        /**
         * Controls how aggressively the packet-size estimate decays after the grace period.
         *
         * On each decay step, `estimate / 2^[PACKET_SIZE_DECAY_SHIFT]` is subtracted from the current
         * estimate. A value of `1` therefore reduces the estimate by approximately half.
         */
        const val PACKET_SIZE_DECAY_SHIFT = 1

        fun encodeEventId(eventId: String): ByteArray {
            val buffer = Unpooled.buffer()
            try {
                buffer.writeString(eventId, EventCodecRegistry.MAX_EVENT_ID_BYTES)
                return ByteBufUtil.getBytes(buffer)
            } finally {
                buffer.release()
            }
        }
    }
}
