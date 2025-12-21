package dev.slne.surf.redis.sync

/**
 * Listener interface for subscribing to changes in synchronized sets.
 * @param T The type of elements in the set
 */
fun interface SyncSetChangeListener<T> {
    /**
     * Called when a change occurs in the synchronized set.
     * @param changeType The type of change that occurred
     * @param value The new or affected value
     */
    fun onChange(changeType: SyncChangeType, value: T)
}
