package dev.slne.surf.redis.sync

import dev.slne.surf.api.core.util.logger
import dev.slne.surf.redis.RedisApi
import dev.slne.surf.redis.RedisInstance
import dev.slne.surf.redis.util.DisposableAware
import org.jetbrains.annotations.MustBeInvokedByOverriders
import reactor.core.publisher.Mono
import reactor.util.function.Tuple2
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.time.Duration


abstract class AbstractSyncStructure<L, R : AbstractSyncStructure.VersionedSnapshot>(
    protected val api: RedisApi,
    id: String,
    override val ttl: Duration
) : DisposableAware(), SyncStructure<L> {
    companion object {
        const val NAMESPACE = "surf-redis:sync:"
        private val log = logger()
    }

    override val id = id.replace(":", "_")

    private val listeners = CopyOnWriteArrayList<(L) -> Unit>()
    protected val lock = ReentrantReadWriteLock()

    @MustBeInvokedByOverriders
    override fun init(): Mono<Void> {
        return loadFromRemote()
            .doOnError { throwable ->
                log.atSevere()
                    .withCause(throwable)
                    .log("Failed to initialize synchronized structure '$id'")
            }
    }

    @MustBeInvokedByOverriders
    override fun dispose0() {
    }

    override fun addListener(listener: (L) -> Unit) {
        listeners += listener
    }

    override fun removeListener(listener: (L) -> Unit) {
        listeners -= listener
    }

    protected fun notifyListeners(value: L) {
        for (listener in listeners) {
            try {
                listener(value)
            } catch (e: Throwable) {
                log.atSevere()
                    .withCause(e)
                    .log("Error notifying listener for $id")
            }
        }
    }

    protected fun loadFromRemote(): Mono<Void> = loadRemoteSnapshot()
        .doOnNext(::overrideFromRemote)
        .then()

    /**
     * Reads the current remote snapshot without applying it locally.
     *
     * The snapshot is emitted on [RedisInstance.streamPollScheduler], never on the Redis I/O thread
     * that completed the reply.
     */
    protected fun loadRemoteSnapshot(): Mono<R> = loadFromRemote0()
        .publishOn(RedisInstance.get().streamPollScheduler)
        .switchIfEmpty(
            Mono.error(
                IllegalStateException("Remote snapshot for synchronized structure '$id' returned no result")
            )
        )

    protected abstract fun loadFromRemote0(): Mono<R>
    protected abstract fun overrideFromRemote(raw: R)


    interface VersionedSnapshot {
        val version: Long
    }

    data class SimpleVersionedSnapshot<V>(
        val value: V,
        override val version: Long
    ) : VersionedSnapshot {
        companion object {
            fun <V : Any> fromTuple(tuple: Tuple2<V, Long>) =
                SimpleVersionedSnapshot(tuple.t1, tuple.t2)
        }
    }
}