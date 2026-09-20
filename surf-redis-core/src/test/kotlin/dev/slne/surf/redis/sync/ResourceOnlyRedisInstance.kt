package dev.slne.surf.redis.sync

import dev.slne.surf.redis.RedisInstance
import java.nio.file.Path

/** Supplies classpath Lua resources to map tests without retaining transport threads. */
class ResourceOnlyRedisInstance : RedisInstance() {
    init {
        disable()
    }

    override val dataPath: Path get() = error("These tests do not load platform configuration")
    override fun tryExtractPluginNameFromClass(clazz: Class<*>) = "map-event-test"
}
