package dev.slne.surf.redis.sync

import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class WakeupPublishScriptTest {
    private val publishCall =
        "redis.call('PUBLISH', '${SyncStreamWakeupBus.CHANNEL}', streamKey .. delim .. originId)"

    @Test
    fun `every stream-appending sync script publishes one wakeup after its XADD`() {
        val root = File(assertNotNull(javaClass.getResource("/lua/sync")).toURI())
        val scripts = root.walk().filter { it.isFile && it.extension == "lua" }
            .associate { it.relativeTo(root).invariantSeparatorsPath to it.readText().replace("\r\n", "\n") }
            .filterValues { "'XADD'" in it }

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
}
