package dev.slne.surf.redis.sync

import dev.slne.surf.redis.RedisApi
import dev.slne.surf.redis.util.DisposableAware
import dev.slne.surf.redis.util.LuaScriptRegistry
import reactor.core.publisher.Mono
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.test.fail

class ResyncSubscriptionTest {

    @Test
    fun `completed resyncs are not retained for the structure lifetime`() = withProbe { probe ->
        val before = trackedDisposables(probe)
        val resyncs = 200
        val deadline = System.nanoTime() + 10_000_000_000L
        while (probe.snapshots.get() < resyncs) {
            if (System.nanoTime() > deadline) fail("only ${probe.snapshots.get()} of $resyncs resyncs started")
            probe.resync()
            Thread.onSpinWait()
        }

        assertTrue(trackedDisposables(probe) - before <= 1, "tracked: ${trackedDisposables(probe)}")
    }

    @Test
    fun `dispose cancels an in-flight resync`() = withProbe { probe ->
        val cancelled = AtomicBoolean()
        probe.remote = { Mono.never<AbstractSyncStructure.SimpleVersionedSnapshot<Unit>>().doOnCancel { cancelled.set(true) } }
        probe.resync()

        probe.dispose()

        assertTrue(cancelled.get())
    }

    private fun trackedDisposables(probe: DisposableAware): Int {
        val field = DisposableAware::class.java.getDeclaredField("disposables").apply { isAccessible = true }
        return (field.get(probe) as Set<*>).size
    }

    private fun withProbe(test: (ResyncProbe) -> Unit) {
        SyncMapEventFixture().use { fixture ->
            val probe = ResyncProbe(fixture.api)
            try { test(probe) } finally { probe.dispose() }
        }
    }

    private class ResyncProbe(api: RedisApi) :
        AbstractStreamSyncStructure<Unit, AbstractSyncStructure.SimpleVersionedSnapshot<Unit>>(
            api, "resync-probe", Duration.ZERO, Scripts, "test:"
        ) {
        val snapshots = AtomicInteger()
        var remote: () -> Mono<SimpleVersionedSnapshot<Unit>> = { Mono.just(SimpleVersionedSnapshot(Unit, 0)) }

        fun resync() = requestResync()

        override fun onStreamEvent(type: String, data: StreamEventData) = Unit
        override fun loadFromRemote0(): Mono<SimpleVersionedSnapshot<Unit>> {
            snapshots.incrementAndGet()
            return remote()
        }

        private object Scripts : LuaScriptRegistry("lua/sync/value") { init { load("set") } }
    }
}
