package dev.slne.surf.redis.test.paper

import com.github.shynixn.mccoroutine.folia.SuspendingJavaPlugin
import com.github.shynixn.mccoroutine.folia.launch
import dev.slne.surf.redis.RedisApi
import dev.slne.surf.redis.sync.syncList
import dev.slne.surf.redis.sync.syncMap
import dev.slne.surf.redis.sync.syncValue
import dev.slne.surf.redis.test.paper.config.RedisConfigManager
import kotlinx.serialization.Serializable
import org.bukkit.plugin.java.JavaPlugin

val plugin get() = JavaPlugin.getPlugin(PaperMain::class.java)

class PaperMain : SuspendingJavaPlugin() {
    lateinit var redisApi: RedisApi

    override fun onLoad() {
        redisApi.init(redisConfigManager.config.url)
    }

    override fun onEnable() {
        if(RedisApi.isConnected()) {
            logger.info("Connected to Redis server at ${redisConfigManager.config.url}")
        } else {
            logger.severe("Failed to connect to Redis server at ${redisConfigManager.config.url}")
        }

        val syncValueString = syncValue("value_string", "default-string-value")
        val syncList = syncList<TestSerializable>("list_serializable")
        val syncMap = syncMap<String, TestSerializable>("map_serializable")

        syncMap.subscribe { type, serializable, any ->
            logger.info("SyncMap Change - Type: $type, Key: $any, Value: $serializable")
        }

        syncList.subscribe { type, serializable, _ ->
            logger.info("SyncList Change - Type: $type, Value: $serializable")
        }

        syncValueString.subscribe { type, string, _ ->
            logger.info("SyncValue Change - Type: $type, Value: $string")
        }

        logger.info("SyncValue: ${syncValueString.get()}")

        plugin.launch {
            syncValueString.set("updated-string-value")
            syncList.add(TestSerializable("first-message", 1))
            syncMap.put("first-key", TestSerializable("map-message", 42))
        }

        logger.info("SyncValue after set: ${syncValueString.get()}")
    }

    @Serializable
    data class TestSerializable(val message: String, val number: Int)
}

val redisConfigManager = RedisConfigManager()