package dev.slne.surf.redis.sync

import com.github.benmanes.caffeine.cache.Caffeine
import dev.slne.surf.api.core.util.logger
import dev.slne.surf.redis.RedisApi
import org.redisson.client.codec.StringCodec
import reactor.core.publisher.Mono
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Dispatches Redis key deletion and expiration events to callbacks registered for the affected key.
 *
 * A single pair of Redis keyevent subscriptions is shared per [RedisApi] instance. Incoming
 * notifications are dispatched locally by key, avoiding the need for every synchronized structure
 * to maintain its own Redis listener.
 *
 * Redis keyspace notifications must be enabled for generic deletion and expiration events,
 * otherwise the registered callbacks will not be invoked.
 *
 * @param subscribe subscribes to a Redis keyevent pattern and invokes the supplied callback with
 * the affected key
 */
internal class SyncKeyEventBus(
    private val subscribe: (pattern: String, onKey: (String) -> Unit) -> Mono<*>,
) {
    companion object {
        private val log = logger()

        /**
         * Redis keyevent pattern matching key deletion events across all logical databases.
         */
        internal const val DELETED_PATTERN = "__keyevent@*:del"

        /**
         * Redis keyevent pattern matching key expiration events across all logical databases.
         */
        internal const val EXPIRED_PATTERN = "__keyevent@*:expired"

        private val byApi = Caffeine.newBuilder()
            .weakKeys()
            .weakValues()
            .build<RedisApi, SyncKeyEventBus>()

        /**
         * Returns the shared keyevent bus associated with the given Redis API instance.
         *
         * The returned bus uses pattern subscriptions for Redis deletion and expiration events.
         *
         * @param api Redis API instance for which to obtain the bus
         * @return the shared keyevent bus for [api]
         */
        fun getInstance(api: RedisApi): SyncKeyEventBus = byApi.get(api) { redisApi ->
            SyncKeyEventBus { pattern, onKey ->
                redisApi.redissonReactive.getPatternTopic(pattern, StringCodec.INSTANCE)
                    .addListener(String::class.java) { _, _, key -> onKey(key) }
            }
        }
    }

    private val subscribers = ConcurrentHashMap<String, CopyOnWriteArrayList<Runnable>>()

    private val initialization: Mono<Void> by lazy {
        Mono.`when`(
            subscribe(DELETED_PATTERN, ::dispatch),
            subscribe(EXPIRED_PATTERN, ::dispatch),
        ).cache()
    }

    /**
     * Initializes the Redis keyevent subscriptions.
     *
     * Initialization is performed at most once for this bus. Subsequent subscriptions to the
     * returned [Mono] reuse the cached initialization result.
     *
     * @return a completion signal for the subscription initialization
     */
    fun init(): Mono<Void> = initialization

    /**
     * Registers a callback to be invoked when the given Redis key is deleted or expires.
     *
     * Multiple callbacks may be registered for the same key.
     *
     * @param key Redis key to observe
     * @param callback callback invoked when a matching keyevent is received
     */
    fun register(key: String, callback: Runnable) {
        subscribers.compute(key) { _, keySubscribers ->
            (keySubscribers ?: CopyOnWriteArrayList()).apply { add(callback) }
        }
    }

    /**
     * Removes a previously registered callback for the given Redis key.
     *
     * If the callback is the last subscriber for the key, the corresponding subscriber entry is
     * removed.
     *
     * @param key Redis key for which the callback was registered
     * @param callback callback to remove
     */
    fun unregister(key: String, callback: Runnable) {
        subscribers.computeIfPresent(key) { _, keySubscribers ->
            keySubscribers.remove(callback)
            keySubscribers.takeUnless { it.isEmpty() }
        }
    }

    private fun dispatch(key: String) {
        val keySubscribers = subscribers[key] ?: return

        for (callback in keySubscribers) {
            try {
                callback.run()
            } catch (t: Throwable) {
                log.atWarning()
                    .withCause(t)
                    .log("Error handling keyevent for '%s'", key)
            }
        }
    }
}
