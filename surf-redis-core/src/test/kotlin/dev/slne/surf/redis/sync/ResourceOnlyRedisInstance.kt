package dev.slne.surf.redis.sync

import dev.slne.surf.redis.RedisInstance
import java.nio.file.Path

/**
 * Supplies classpath Lua resources to map tests without retaining transport threads.
 *
 * [streamPollScheduler] stays alive: stream handlers and snapshot installation are published on it.
 */
class ResourceOnlyRedisInstance : RedisInstance() {
    init {
        ttlRefreshScheduler.dispose()
        eventLoopGroup.shutdownGracefully().syncUninterruptibly()
        redissonExecutorService.shutdownNow()
    }

    override val dataPath: Path get() = error("These tests do not load platform configuration")
    override fun tryExtractPluginNameFromClass(clazz: Class<*>) = "map-event-test"
}
