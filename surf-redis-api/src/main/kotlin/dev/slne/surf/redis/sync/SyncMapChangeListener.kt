package dev.slne.surf.redis.sync

/**
 * Listener interface for subscribing to changes in synchronized maps.
 * @param K The type of keys in the map
 * @param V The type of values in the map
 */
fun interface SyncMapChangeListener<K, V> {
    /**
     * Called when a change occurs in the synchronized map.
     * @param changeType The type of change that occurred (SET or REMOVE)
     * @param key The key that was affected
     * @param value The new value (for SET) or the removed value (for REMOVE)
     */
    fun onChange(changeType: SyncChangeType, key: K, value: V)
}
