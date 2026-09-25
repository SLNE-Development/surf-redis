package dev.slne.surf.redis.util

import org.redisson.api.RKeysReactive
import reactor.core.publisher.Mono
import java.lang.reflect.Proxy
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import java.time.Duration as JavaDuration

/**
 * Observes only the first refresh tick: the proxied `expire` completes synchronously, so that tick has run when
 * `refreshContinuously` returns, and the returned subscription is disposed before a second one could start.
 */
class RedisExpirableUtilsTest {
    private val batches = mutableListOf<Pair<JavaDuration, List<String>>>()

    private val redisKeys = Proxy.newProxyInstance(
        RKeysReactive::class.java.classLoader, arrayOf(RKeysReactive::class.java)
    ) { _, method, args ->
        when (method.name) {
            "expire" if args?.size == 2 && args[1] is Array<*> -> Mono.fromCallable {
                @Suppress("UNCHECKED_CAST")
                batches += (args[0] as JavaDuration) to (args[1] as Array<String>).toList()
                batches.last().second.size.toLong()
            }

            else -> error("Unexpected keys operation ${method.name}")
        }
    } as RKeysReactive

    @Test
    fun `refreshes every key of a structure in one batch on the first tick`() {
        RedisExpirableUtils.refreshContinuously(redisKeys, 1.minutes, "snapshot", "stream", "version", "codec")
            .dispose()

        assertEquals(listOf(JavaDuration.ofMinutes(1) to listOf("snapshot", "stream", "version", "codec")), batches)
    }

    @Test
    fun `never expires keys when the ttl is not positive`() {
        RedisExpirableUtils.refreshContinuously(redisKeys, Duration.ZERO, "snapshot").dispose()
        RedisExpirableUtils.refreshContinuously(redisKeys, (-1).minutes, "snapshot").dispose()

        assertTrue(batches.isEmpty())
    }
}
