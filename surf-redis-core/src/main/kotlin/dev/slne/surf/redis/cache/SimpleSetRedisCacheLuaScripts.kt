package dev.slne.surf.redis.cache

import dev.slne.surf.redis.RedisApi
import dev.slne.surf.redis.util.LuaScriptExecutor
import org.redisson.api.RScript

/**
 * Provides Lua script execution for cache operations.
 * Scripts are now loaded from resources/lua/cache/*.lua
 */
object SimpleSetRedisCacheLuaScripts {
    private val registry = CacheLuaScriptRegistry.getInstance()

    fun execute(
        api: RedisApi,
        scriptName: String,
        mode: RScript.Mode,
        returnType: RScript.ReturnType,
        keys: List<Any>,
        vararg values: Any
    ): Any? {
        val executor = LuaScriptExecutor.getInstance(api, registry)
        return executor.execute<Any>(
            scriptName,
            mode,
            returnType,
            keys,
            *values
        )
    }

    suspend fun <R : Any> executeSuspend(
        api: RedisApi,
        scriptName: String,
        mode: RScript.Mode,
        returnType: RScript.ReturnType,
        keys: List<Any>,
        vararg values: Any
    ): R {
        val executor = LuaScriptExecutor.getInstance(api, registry)
        return executor.execute(
            scriptName,
            mode,
            returnType,
            keys,
            *values
        )
    }
}