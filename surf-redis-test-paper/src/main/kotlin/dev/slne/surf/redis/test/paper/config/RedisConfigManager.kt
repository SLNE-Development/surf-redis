package dev.slne.surf.redis.test.paper.config

import dev.slne.surf.redis.test.paper.plugin
import dev.slne.surf.surfapi.core.api.config.manager.SpongeConfigManager
import dev.slne.surf.surfapi.core.api.config.surfConfigApi

class RedisConfigManager {
    private val configManager: SpongeConfigManager<RedisConfig>

    init {
        surfConfigApi.createSpongeYmlConfig(
            RedisConfig::class.java,
            plugin.dataPath,
            "config.yml"
        )
        configManager = surfConfigApi.getSpongeConfigManagerForConfig(
            RedisConfig::class.java
        )
        reload()
    }

    fun reload() {
        configManager.reloadFromFile()
    }

    val config get() = configManager.config
}