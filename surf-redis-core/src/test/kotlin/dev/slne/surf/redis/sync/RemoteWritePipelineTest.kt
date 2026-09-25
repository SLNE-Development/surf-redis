package dev.slne.surf.redis.sync

import org.redisson.api.RScript
import org.redisson.client.RedisNoScriptException
import reactor.core.Exceptions
import reactor.core.publisher.Mono
import kotlin.test.*

class RemoteWritePipelineTest {
    @Test
    fun `owned argument arrays stay isolated across deferred writes retries and resubscriptions`() = withProbe { fixture, probe ->
        val first = arrayOf<Any>("first", 1L)
        val second = arrayOf<Any>("second", 2L)
        val captured = mutableListOf<List<Any?>>()
        fixture.onScriptArguments = { args ->
            captured += args.toList()
            @Suppress("UNCHECKED_CAST")
            (args as Array<Any>)[0] = "changed by transport"
        }
        var attempts = 0
        fixture.scriptResponse = {
            if (++attempts == 1) Mono.error<Long>(RedisNoScriptException("missing")) else Mono.just(0L)
        }
        fun command(values: Array<Any>) = probe.executor.executeWithValues<Long>(
            "set", RScript.Mode.READ_WRITE, RScript.ReturnType.LONG, listOf("key"), values
        )
        val a = command(first)
        val b = command(second)
        assertEquals(0, fixture.scriptLoads)
        b.block()
        a.block()
        b.block()
        val expected: List<List<Any?>> = listOf(second.toList(), second.toList(), first.toList(), second.toList())
        assertEquals(expected, captured)
        assertEquals("first", first[0])
        assertEquals("second", second[0])
        assertEquals(2, fixture.scriptLoads)
    }

    @Test
    fun `every write variant preserves the Lua envelope and payload arguments`() = withProbe { fixture, probe ->
        for (values in listOf(emptyArray(), arrayOf("", "ä\u0000tail"), Array(64) { "value-$it" })) {
            var captured = emptyList<Any?>()
            fixture.onScriptArguments = { captured = it.toList() }
            val expected = listOf(probe.origin, "\u0000", "10000", "T", "M", "SET") + values
            fixture.scriptResponse = { Mono.just(0L) }
            assertEquals(0L, probe.write(values).block())
            assertEquals(expected, captured)
            probe.fire(values)
            assertEquals(expected, captured)
            assertFalse(probe.apply(values))
            assertEquals(expected, captured)
            fixture.scriptResponse = { Mono.just(listOf(0L, 0L)) }
            assertEquals(0L to 0L, probe.batch(values).block())
            assertEquals(expected, captured)
            probe.fireBatch(values)
            assertEquals(expected, captured)
            fixture.scriptResponse = { Mono.just(listOf(0L, "", "extra")) }
            assertEquals("extra", probe.payload(values))
            assertEquals(expected, captured)
        }
        assertEquals(1, fixture.scriptLoads)
        assertEquals(0, fixture.wakeups)
    }

    @Test
    fun `write results preserve local events and negative-version resync without client wakeups`() = withProbe { fixture, probe ->
        probe.bootstrap()
        fixture.scriptResponse = { Mono.just(1L) }
        assertTrue(probe.apply(arrayOf("first")))
        assertEquals("first", probe.appliedPayload)
        fixture.scriptResponse = { Mono.just(listOf(2L, "second", "old")) }
        assertEquals("old", probe.payload(arrayOf("second")))
        assertEquals("second", probe.appliedPayload)
        fixture.scriptResponse = { Mono.just(listOf(3L, 4L)) }
        assertEquals(3L to 4L, probe.batch(arrayOf("third", "fourth")).block())
        assertEquals(0, probe.snapshots)
        fixture.scriptResponse = { Mono.just(-1L) }
        assertEquals(-1L, probe.write(emptyArray()).block())
        assertEquals(1, probe.snapshots)
        // The mutation scripts publish the wakeup themselves
        assertEquals(0, fixture.wakeups)
    }

    @Test
    fun `cold execution reloads NOSCRIPT and gives each subscription its own retry budget`() = withProbe { fixture, probe ->
        var attempts = 0
        fixture.scriptResponse = {
            attempts++
            if (attempts % 4 != 0) Mono.error<Long>(RedisNoScriptException("missing")) else Mono.just(7L)
        }
        val command = probe.executor.execute<Long>(
            "set", RScript.Mode.READ_WRITE, RScript.ReturnType.LONG, listOf("key"), "value"
        )
        assertEquals(0, fixture.scriptLoads)
        assertEquals(7L, command.block())
        assertEquals(7L, command.block())
        assertEquals(8, attempts)
        assertEquals(7, fixture.scriptLoads)
    }

    @Test
    fun `custom retry budgets stop at the configured attempt and preserve exhaustion cause`() = withProbe { fixture, probe ->
        val missing = RedisNoScriptException("missing")
        fixture.scriptResponse = { Mono.error<Long>(missing) }
        for (tries in listOf(0, 1, 3, 5)) {
            val before = fixture.resyncs.get()
            val failure = assertFailsWith<IllegalStateException> {
                probe.executor.execute<Long>(
                    "set", RScript.Mode.READ_WRITE, RScript.ReturnType.LONG, emptyList(), tries = tries
                ).block()
            }
            assertTrue(Exceptions.isRetryExhausted(failure))
            assertSame(missing, failure.cause)
            assertEquals(tries + 1, fixture.resyncs.get() - before)
        }
    }

    @Test
    fun `other script errors are not retried or evicted`() = withProbe { fixture, probe ->
        val broken = IllegalArgumentException("bad arguments")
        fixture.scriptResponse = { Mono.error<Long>(broken) }
        repeat(2) {
            assertSame(broken, assertFailsWith<IllegalArgumentException> {
                probe.executor.execute<Long>(
                    "set", RScript.Mode.READ_WRITE, RScript.ReturnType.LONG, emptyList()
                ).block()
            })
        }
        assertEquals(2, fixture.resyncs.get())
        assertEquals(1, fixture.scriptLoads)
    }

    private fun withProbe(test: (SyncMapEventFixture, RemoteWriteProbe) -> Unit) {
        SyncMapEventFixture().use { fixture ->
            val probe = RemoteWriteProbe(fixture.api)
            try { test(fixture, probe) } finally { probe.dispose() }
        }
    }
}
