package dev.slne.surf.redis.test.paper.config

import org.spongepowered.configurate.objectmapping.ConfigSerializable

@ConfigSerializable
data class RedisConfig(
    val url: String = "redis://localhost:6379",
)