@file:OptIn(ExperimentalContracts::class)

package dev.slne.surf.redis.sync

import java.util.concurrent.locks.StampedLock
import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.InvocationKind
import kotlin.contracts.contract

/**
 * Provides synchronized access to a shared structure using a [StampedLock].
 *
 * Supports regular reads, optimistic reads, and exclusive writes. Write access is
 * reentrant for the thread currently owning the write lock. Reads performed by the
 * write-owning thread also execute directly without acquiring an additional lock.
 */
internal class SyncStructureLock {

    /**
     * The underlying lock used to coordinate read and write access.
     */
    @PublishedApi
    internal val stamped = StampedLock()

    /**
     * The thread currently owning the write lock, or `null` when no write operation
     * is active.
     */
    @PublishedApi
    @Volatile
    internal var owner: Thread? = null

    /**
     * Executes [block] while holding a read lock.
     *
     * If the current thread already owns the write lock, [block] is executed directly
     * without acquiring a read lock.
     *
     * [block] is invoked exactly once.
     *
     * @param block the operation to execute while read access is protected.
     * @return the value returned by [block].
     */
    inline fun <T> read(block: () -> T): T {
        contract {
            callsInPlace(block, InvocationKind.EXACTLY_ONCE)
        }

        if (owner === Thread.currentThread()) return block()

        val stamp = stamped.readLock()
        try {
            return block()
        } finally {
            stamped.unlockRead(stamp)
        }
    }

    /**
     * Executes [block] using an optimistic read when possible.
     *
     * The result of the optimistic execution is returned when no conflicting write
     * occurred while [block] was running. If the optimistic read is invalidated,
     * [block] is retried while holding a regular read lock.
     *
     * If [block] throws during an optimistic read and the optimistic stamp is still
     * valid, the exception is propagated. If the stamp was invalidated, the operation
     * is retried under a read lock instead.
     *
     * If the current thread already owns the write lock, [block] is executed directly.
     *
     * Because an invalidated optimistic read causes a retry, [block] may be invoked
     * more than once and should therefore avoid externally visible side effects.
     *
     * During the optimistic attempt, [block] may observe arbitrarily inconsistent state
     * and must terminate regardless; exceptions are recovered, non-termination is not.
     * Suitable blocks read only structures with bounded lookup paths, such as fastutil
     * open-addressing collections or array lists, and never iterate or mutate.
     *
     * @param block the operation to execute optimistically.
     * @return the value produced by a validated optimistic read or by the fallback
     * read operation.
     */
    inline fun <T> optimisticRead(block: () -> T): T {
        contract {
            callsInPlace(block, InvocationKind.AT_LEAST_ONCE)
        }

        if (owner === Thread.currentThread()) return block()

        val stamp = stamped.tryOptimisticRead()
        if (stamp != 0L) {
            try {
                val result = block()
                if (stamped.validate(stamp)) return result
            } catch (e: Throwable) {
                if (stamped.validate(stamp)) throw e
            }
        }

        return read(block)
    }

    /**
     * Executes [block] while holding the exclusive write lock.
     *
     * Write access is reentrant for the thread currently owning the write lock. A
     * nested invocation from that thread executes [block] directly instead of
     * attempting to acquire the non-reentrant [StampedLock] again.
     *
     * [block] is invoked exactly once.
     *
     * @param block the operation to execute while exclusive write access is held.
     * @return the value returned by [block].
     */
    inline fun <T> write(block: () -> T): T {
        contract {
            callsInPlace(block, InvocationKind.EXACTLY_ONCE)
        }

        val current = Thread.currentThread()
        if (owner === current) return block()

        val stamp = stamped.writeLock()
        owner = current

        try {
            return block()
        } finally {
            owner = null
            stamped.unlockWrite(stamp)
        }
    }
}
