package dev.slne.surf.redis.sync

/**
 * Listener interface for subscribing to changes in synchronized values.
 * @param T The type of the value
 */
fun interface SyncValueChangeListener<T> {
    /**
     * Called when a change occurs in the synchronized value.
     * @param changeType The type of change that occurred
     * @param value The new value
     */
    fun onChange(changeType: SyncChangeType, value: T)
}
