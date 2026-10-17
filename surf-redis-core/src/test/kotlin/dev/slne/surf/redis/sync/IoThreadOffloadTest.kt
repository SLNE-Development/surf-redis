package dev.slne.surf.redis.sync

import dev.slne.surf.redis.RedisApi
import dev.slne.surf.redis.util.LuaScriptRegistry
import dev.slne.surf.redis.util.pollContinuously
import org.redisson.api.RStreamReactive
import org.redisson.api.stream.StreamMessageId
import org.redisson.api.stream.StreamReadArgs
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import java.lang.reflect.Proxy
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.*
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours

/** Slow stream handlers and snapshot installation must not occupy the thread that completed the Redis reply. */
class IoThreadOffloadTest {
    private val io: Scheduler = Schedulers.newSingle("redisson-netty-thread-fake")

    @AfterTest
    fun tearDown() = io.dispose()

    /** Returns whether [io] can run another task while the caller is still busy. */
    private fun ioLoopIsFree(): Boolean {
        val ran = CountDownLatch(1)
        io.schedule { ran.countDown() }
        return ran.await(2, TimeUnit.SECONDS)
    }

    @Test
    fun `stream batches are handled in order off the reply thread one at a time`() {
        val batches = (1L..3L).map { mapOf(StreamMessageId(it, 0) to mapOf("T" to "type", "M" to "msg-$it")) }
        val reads = AtomicInteger()
        val handling = AtomicBoolean()
        val overlaps = AtomicInteger()
        val stream = fakeStream { _ ->
            if (handling.get()) overlaps.incrementAndGet()
            val index = reads.getAndIncrement()
            if (index < batches.size) Mono.just(batches[index]).publishOn(io) else Mono.just(emptyMap<StreamMessageId, Map<String, String>>()).publishOn(io)
        }
        val handled = CopyOnWriteArrayList<StreamMessageId>()
        val ioFree = CopyOnWriteArrayList<Boolean>()
        val done = CountDownLatch(batches.size)
        val wakeups = Sinks.many().unicast().onBackpressureBuffer<Unit>()
        val cursor = AtomicReference(StreamMessageId(0, 0))

        val poll = stream.pollContinuously(cursor, wakeups.asFlux(), pollInterval = 1.hours) {
            onSuccess { batch ->
                handling.set(true)
                try {
                    ioFree += ioLoopIsFree()
                    handled += batch.keys
                    cursor.set(batch.keys.last())
                } finally {
                    handling.set(false)
                    done.countDown()
                }
                wakeups.tryEmitNext(Unit)
            }
            onFailure { throw AssertionError(it) }
        }
        try {
            assertTrue(done.await(10, TimeUnit.SECONDS), "handled ${handled.size} of ${batches.size} batches")
        } finally {
            poll.dispose()
        }

        assertEquals(batches.map { it.keys.single() }, handled)
        assertEquals(List(batches.size) { true }, ioFree)
        assertEquals(0, overlaps.get())
        assertEquals(StreamMessageId(3, 0), cursor.get())
    }

    @Test
    fun `remote snapshots are installed off the reply thread`() {
        SyncMapEventFixture().use { fixture ->
            val probe = SnapshotThreadProbe(fixture.api, io)
            try {
                probe.resync()
                assertEquals(true, probe.ioFreeDuringInstall)
            } finally {
                probe.dispose()
            }
        }
    }

    private fun fakeStream(
        read: (StreamReadArgs) -> Mono<Map<StreamMessageId, Map<String, String>>>,
    ): RStreamReactive<String, String> {
        @Suppress("UNCHECKED_CAST")
        return Proxy.newProxyInstance(
            RStreamReactive::class.java.classLoader,
            arrayOf(RStreamReactive::class.java),
        ) { _, method, args ->
            when {
                method.name == "read" && args?.singleOrNull() is StreamReadArgs -> read(args[0] as StreamReadArgs)
                else -> throw UnsupportedOperationException(method.toString())
            }
        } as RStreamReactive<String, String>
    }

    private inner class SnapshotThreadProbe(api: RedisApi, private val io: Scheduler) :
        AbstractStreamSyncStructure<Unit, AbstractSyncStructure.SimpleVersionedSnapshot<Unit>>(
            api, "offload-probe", Duration.ZERO, object : LuaScriptRegistry("unused") {}, "test:"
        ) {
        @Volatile
        var ioFreeDuringInstall: Boolean? = null

        fun resync() {
            loadFromRemote().block(java.time.Duration.ofSeconds(10))
        }

        override fun overrideFromRemote(raw: SimpleVersionedSnapshot<Unit>) {
            super.overrideFromRemote(raw)
            ioFreeDuringInstall = ioLoopIsFree()
        }

        override fun onStreamEvent(type: String, data: StreamEventData) = Unit
        override fun loadFromRemote0(): Mono<SimpleVersionedSnapshot<Unit>> =
            Mono.just(SimpleVersionedSnapshot(Unit, 1)).publishOn(io)
    }
}
