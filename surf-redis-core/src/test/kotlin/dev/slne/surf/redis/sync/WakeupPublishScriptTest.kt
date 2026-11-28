package dev.slne.surf.redis.sync

import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class WakeupPublishScriptTest {
    private val publishCall =
        "redis.call('PUBLISH', '${SyncStreamWakeupBus.CHANNEL}', streamKey .. delim .. originId)"

    private fun streamAppendingScripts(resource: String): Map<String, String> {
        val root = File(assertNotNull(javaClass.getResource(resource)).toURI())
        return root.walk().filter { it.isFile && it.extension == "lua" }
            .associate { it.relativeTo(root).invariantSeparatorsPath to it.readText().replace("\r\n", "\n") }
            .filterValues { "'XADD'" in it }
    }

    @Test
    fun `every stream-appending sync script publishes one wakeup after its XADD`() {
        val scripts = streamAppendingScripts("/lua/sync")

        assertEquals(25, scripts.size)
        for ((path, script) in scripts) {
            val lines = script.lines()
            val publish = lines.indexOfFirst { it.trim() == publishCall }

            assertEquals(1, lines.count { "PUBLISH" in it }, path)
            assertTrue(publish > lines.indexOfLast { "'XADD'" in it }, path)

            if (path.endsWith("remove-many.lua")) {
                assertEquals("if lastVersion > 0 then", lines[publish - 1], path)
            }
        }
    }

    @Test
    fun `every stream-appending cache script publishes one wakeup at top level after its last XADD`() {
        val scripts = streamAppendingScripts("/lua/cache")

        assertEquals(8, scripts.size)
        for ((path, script) in scripts) {
            val lines = script.lines()
            val publish = lines.indexOf(publishCall)

            assertEquals(1, lines.count { "PUBLISH" in it }, path)
            assertTrue(publish > lines.indexOfLast { "'XADD'" in it }, path)
        }
    }
}
