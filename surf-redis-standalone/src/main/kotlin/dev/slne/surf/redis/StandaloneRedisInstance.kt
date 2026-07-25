package dev.slne.surf.redis

import com.google.auto.service.AutoService
import dev.slne.surf.redis.config.RedisConfig
import java.nio.file.Path
import kotlin.properties.Delegates

class StandaloneRedisInstance(
    val name: String,
    private val configPath: Path,
) {

    fun create(host: String, port: Int, password: String?) {
        InstanceImpl.pluginName = name
        InstanceImpl.pluginDataPath = configPath

        InstanceImpl.host = host
        InstanceImpl.port = port
        InstanceImpl.password = password

        RedisInstance.instance.load()
    }

    fun shutdown() {
        RedisInstance.instance.disable()
    }

    @AutoService(RedisInstance::class)
    class InstanceImpl : RedisInstance() {

        companion object {
            lateinit var pluginName: String
            lateinit var pluginDataPath: Path

            lateinit var host: String
            var port by Delegates.notNull<Int>()
            var password: String? = null
        }

        override val dataPath: Path
            get() = pluginDataPath

        override val config = RedisConfig(
            host = host,
            port = port,
            password = password
        )

        override fun load() = Unit

        override fun tryExtractPluginNameFromClass(clazz: Class<*>) =
            pluginName
    }
}