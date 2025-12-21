package dev.slne.surf.redis.example.request

import dev.slne.surf.redis.request.RedisRequest
import dev.slne.surf.redis.request.RedisResponse
import kotlinx.serialization.Serializable

/**
 * Example request: Get players with a minimum level
 */
@Serializable
data class GetPlayerRequest(val minLevel: Int) : RedisRequest()

/**
 * Example response: List of player names
 */
@Serializable
data class PlayerListResponse(val players: List<String>) : RedisResponse()

/**
 * Example request: Get server status
 */
@Serializable
data class ServerStatusRequest(val serverName: String) : RedisRequest()

/**
 * Example response: Server status information
 */
@Serializable
data class ServerStatusResponse(
    val serverName: String,
    val online: Boolean,
    val playerCount: Int
) : RedisResponse()
