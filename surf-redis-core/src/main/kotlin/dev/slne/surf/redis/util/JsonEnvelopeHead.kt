package dev.slne.surf.redis.util

import java.util.*

/**
 * Reads the routing field of a JSON pub/sub envelope without parsing the rest of the message.
 */
internal object JsonEnvelopeHead {

    /** Returns the `{"<key>":"` prefix that the readers of this object expect. */
    fun prefix(key: String): String = "{\"$key\":\""

    /** Returns the leading string field's value, or `null` if the message does not start with [prefix]. */
    fun leadingString(message: String, prefix: String): String? {
        if (!message.startsWith(prefix)) return null
        val end = closingQuote(message, prefix.length)
        return if (end < 0) null else message.substring(prefix.length, end)
    }

    /**
     * Returns the value of the string field that directly follows the leading string field.
     *
     * [separator] is the text between both values, starting with the leading value's closing
     * quote, e.g. `","request_class":"`.
     */
    fun secondString(message: String, prefix: String, separator: String): String? {
        if (!message.startsWith(prefix)) return null
        val firstEnd = closingQuote(message, prefix.length)
        if (firstEnd < 0 || !message.startsWith(separator, firstEnd)) return null
        val start = firstEnd + separator.length
        val end = closingQuote(message, start)
        return if (end < 0) null else message.substring(start, end)
    }

    /**
     * Returns the leading field as a [UUID] in its canonical 36-character form.
     *
     * Other forms return `null`.
     */
    fun leadingUuid(message: String, prefix: String): UUID? {
        val raw = leadingString(message, prefix) ?: return null
        if (raw.length != 36) return null
        return try {
            UUID.fromString(raw)
        } catch (_: IllegalArgumentException) {
            null
        }
    }

    private fun closingQuote(message: String, from: Int): Int {
        for (i in from until message.length) {
            when (message[i]) {
                '"' -> return i
                '\\' -> return -1
            }
        }
        return -1
    }
}
