package dev.slne.surf.redis.sync

import io.lettuce.core.RedisClient
import io.lettuce.core.pubsub.RedisPubSubListener
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection
import kotlinx.coroutines.*
import kotlinx.coroutines.future.await
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Base class for all synchronized structures.
 * Handles Redis pub/sub communication and change notifications.
 * @param L The listener type for this structure
 */
abstract class SyncStructure<L>(
    protected val id: String,
    redisUri: String,
    private val coroutineScope: CoroutineScope = CoroutineScope(Dispatchers.Default + SupervisorJob())
) {
    protected val client: RedisClient = RedisClient.create(redisUri)
    protected val pubConnection: StatefulRedisPubSubConnection<String, String> = client.connectPubSub()
    protected val subConnection: StatefulRedisPubSubConnection<String, String> = client.connectPubSub()
    
    // Thread-safe list for listeners
    protected val listeners = CopyOnWriteArrayList<L>()
    
    protected abstract val channelPrefix: String
    
    protected val channel: String
        get() = "$channelPrefix:$id"
    
    init {
        setupSubscription()
    }
    
    private fun setupSubscription() {
        subConnection.addListener(object : RedisPubSubListener<String, String> {
            override fun message(channel: String, message: String) {
                if (channel == this@SyncStructure.channel) {
                    coroutineScope.launch {
                        handleIncomingMessage(message)
                    }
                }
            }
            
            override fun message(pattern: String, channel: String, message: String) {}
            override fun subscribed(channel: String, count: Long) {}
            override fun psubscribed(pattern: String, count: Long) {}
            override fun unsubscribed(channel: String, count: Long) {}
            override fun punsubscribed(pattern: String, count: Long) {}
        })
        
        subConnection.sync().subscribe(channel)
    }
    
    protected abstract suspend fun handleIncomingMessage(message: String)
    
    protected suspend fun publishMessage(message: String) {
        withContext(Dispatchers.IO) {
            pubConnection.async().publish(channel, message).await()
        }
    }
    
    /**
     * Subscribe to changes in this synchronized structure.
     * @param listener The listener to be notified of changes
     */
    fun subscribe(listener: L) {
        listeners.add(listener)
    }
    
    /**
     * Unsubscribe from changes in this synchronized structure.
     * @param listener The listener to remove
     */
    fun unsubscribe(listener: L) {
        listeners.remove(listener)
    }
    
    /**
     * Close the Redis connections and clean up resources.
     */
    open fun close() {
        pubConnection.close()
        subConnection.close()
        client.shutdown()
        coroutineScope.cancel()
    }
    
    @Serializable
    protected data class SyncMessage(
        val operation: String,
        val data: String,
        val key: String? = null
    )
}
