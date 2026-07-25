package dev.slne.surf.redis

import com.google.auto.service.AutoService
import dev.slne.surf.redis.config.RedisConfig
import io.papermc.paper.plugin.provider.classloader.ConfiguredPluginClassLoader

@AutoService(RedisInstance::class)
class RedisInstanceImpl : RedisInstance() {
    override val dataPath get() = PaperBootstrap.dataPath

    override val config: RedisConfig
        get() = RedisConfig.getConfig()

    @Suppress("UnstableApiUsage")
    override fun tryExtractPluginNameFromClass(clazz: Class<*>): String {
        val classloader = clazz.classLoader
        if (classloader !is ConfiguredPluginClassLoader) {
            return clazz.simpleName
        }

        return classloader.configuration.name
    }

    override fun load() {
        RedisConfig.init()
    }
}