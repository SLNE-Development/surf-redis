package dev.slne.surf.redis.cache

import dev.slne.surf.redis.util.LuaScriptRegistry

/**
 * Registry for cache-related Lua scripts.
 * Scripts are loaded from classpath: /lua/cache/*.lua
 */
class CacheLuaScriptRegistry private constructor() : LuaScriptRegistry("lua.cache") {
    companion object {
        const val UPSERT = "upsert"
        const val REMOVE_BY_ID = "remove_by_id"
        const val REMOVE_BY_INDEX = "remove_by_index"
        const val TOUCH_VALUE = "touch_value"

        private val INSTANCE = CacheLuaScriptRegistry()

        fun getInstance(): CacheLuaScriptRegistry = INSTANCE
    }

    init {
        load(UPSERT)
        load(REMOVE_BY_ID)
        load(REMOVE_BY_INDEX)
        load(TOUCH_VALUE)
    }
}