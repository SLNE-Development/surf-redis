package dev.slne.surf.redis.sync

/**
 * Listener interface for subscribing to changes in synchronized lists.
 * @param T The type of elements in the list
 */
fun interface SyncListChangeListener<T> {
    /**
     * Called when a change occurs in the synchronized list.
     * @param changeType The type of change that occurred (ADD, SET, or REMOVE)
     * @param value The new value (for ADD/SET) or the removed value (for REMOVE)
     * @param index The position in the list: provided for ADD_AT, SET, and REMOVE_AT operations; null for general ADD and REMOVE operations
     */
    fun onChange(changeType: SyncChangeType, value: T, index: Int?)
}
