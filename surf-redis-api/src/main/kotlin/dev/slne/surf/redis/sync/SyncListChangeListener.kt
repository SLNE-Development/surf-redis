package dev.slne.surf.redis.sync

/**
 * Listener interface for subscribing to changes in synchronized lists.
 * @param T The type of elements in the list
 */
fun interface SyncListChangeListener<T> {
    /**
     * Called when a change occurs in the synchronized list.
     * @param changeType The type of change that occurred
     * @param value The new or affected value
     * @param index Optional index for operations that affect a specific position (null for general operations)
     */
    fun onChange(changeType: SyncChangeType, value: T, index: Int?)
}
