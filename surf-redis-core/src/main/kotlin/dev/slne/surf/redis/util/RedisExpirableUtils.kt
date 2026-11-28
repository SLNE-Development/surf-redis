package dev.slne.surf.redis.util

import dev.slne.surf.api.core.util.logger
import dev.slne.surf.redis.RedisInstance
import org.redisson.api.RKeysReactive
import reactor.core.Disposable
import reactor.core.Disposables
import reactor.core.publisher.Mono
import kotlin.time.Duration
import kotlin.time.toJavaDuration

object RedisExpirableUtils {
    private val log = logger()

    /**
     * Resets the TTL of [keys] to [ttl] immediately and then periodically until the returned
     * [Disposable] is disposed.
     */
    fun refreshContinuously(
        redisKeys: RKeysReactive,
        ttl: Duration,
        vararg keys: String
    ): Disposable {
        if (ttl == Duration.ZERO || ttl.isNegative()) return Disposables.disposed()
        val delay = Math.clamp(ttl.inWholeSeconds - 2, 1L, 15L)
        val objectNames = keys.joinToString(", ")
        val refresh = redisKeys.expire(ttl.toJavaDuration(), *keys).then()
        val interval = Mono.delay(
            java.time.Duration.ofSeconds(delay),
            RedisInstance.get().ttlRefreshScheduler
        )

        return refresh
            .onErrorResume { e ->
                log.atWarning()
                    .withCause(e)
                    .log("Failed to refresh TTL for Redis expirable objects: $objectNames")
                Mono.empty()
            }
            .then(interval)
            .repeat()
            .subscribe()
    }
}
