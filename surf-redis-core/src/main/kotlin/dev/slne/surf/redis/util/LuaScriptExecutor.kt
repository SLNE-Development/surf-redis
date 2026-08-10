package dev.slne.surf.redis.util

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.LoadingCache
import dev.slne.surf.redis.RedisApi
import org.redisson.api.RScript
import org.redisson.client.RedisNoScriptException
import org.redisson.client.codec.StringCodec
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import java.util.concurrent.ConcurrentHashMap

class LuaScriptExecutor private constructor(private val api: RedisApi, private val registry: LuaScriptRegistry) {

    private val scriptShas = ConcurrentHashMap<String, Mono<String>>()
    private val script by lazy { api.redissonReactive.getScript(StringCodec.INSTANCE) }

    private fun getSha(id: String): Mono<String> = scriptShas.computeIfAbsent(id) { key ->
        script.scriptLoad(registry.get(id))
            .cacheInvalidateIf { false }
    }

    fun <R : Any> execute(
        id: String,
        mode: RScript.Mode,
        returnType: RScript.ReturnType,
        keys: List<Any>,
        vararg values: Any,
        attempts: Int = 3
    ): Mono<R> {
        require(attempts >= 1) { "attempts must be at least 1" }

        val execution = Mono.defer {
            val cachedSha = getSha(id)

            cachedSha.flatMap { sha ->
                script.evalSha<R>(
                    mode,
                    sha,
                    returnType,
                    keys,
                    *values
                ).doOnError(RedisNoScriptException::class.java) {
                    scriptShas.remove(id, cachedSha)
                }
            }
        }

        if (attempts == 1) {
            return execution
        }

        return execution.retryWhen(
            Retry.max(attempts - 1L)
                .filter(RedisNoScriptException::class.java::isInstance)
                .onRetryExhaustedThrow { _, signal ->
                    signal.failure()
                }
        )
    }

    companion object {
        private val byApi = Caffeine.newBuilder()
            .weakKeys()
            .build<RedisApi, Cache<LuaScriptRegistry, LuaScriptExecutor>>()


        fun getInstance(api: RedisApi, registry: LuaScriptRegistry): LuaScriptExecutor {
            val byRegistry = byApi.get(api) {
                Caffeine.newBuilder()
                    .weakKeys()
                    .weakValues()
                    .build()
            }

            return byRegistry.get(registry) { LuaScriptExecutor(api, registry) }
        }
    }
}
