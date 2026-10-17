package dev.slne.surf.redis.util

import org.redisson.api.RStreamReactive
import org.redisson.api.stream.StreamMessageId
import org.redisson.api.stream.StreamReadArgs
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import java.lang.reflect.Proxy
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.hours

/**
 * Pins how one poll trigger drains a stream backlog: peers must converge on a bulk mutation at round-trip speed,
 * without re-reading entries the handler did not consume and without reading past a resync.
 * The fallback interval is one hour, so every read comes from the initial trigger or an explicit wakeup.
 */
class PollContinuouslyDrainTest {
    private val batchSize = 200
    private val cursor = AtomicReference(StreamMessageId(0, 0))
    private val wakeups = Sinks.many().unicast().onBackpressureBuffer<Unit>()
    private val reads = AtomicInteger()

    /** Serves up to [batchSize] of [entries] after the current [cursor]; fails the read numbered [failAt]. */
    private fun backlog(entries: Int, failAt: Int = -1): RStreamReactive<String, String> {
        val ids = (1L..entries).map { StreamMessageId(it, 0) }

        @Suppress("UNCHECKED_CAST")
        return Proxy.newProxyInstance(
            RStreamReactive::class.java.classLoader,
            arrayOf(RStreamReactive::class.java),
        ) { _, method, args ->
            check(method.name == "read" && args?.singleOrNull() is StreamReadArgs) { method.toString() }
            val read = reads.incrementAndGet()
            val after = cursor.get().id0
            val batch = ids.filter { it.id0 > after }.take(batchSize).associateWith { mapOf("M" to "msg-${it.id0}") }
            if (read == failAt) Mono.error(IllegalStateException("read $read failed")) else Mono.just(batch)
        } as RStreamReactive<String, String>
    }

    private fun settle() = Thread.sleep(300)

    @Test
    fun `one trigger drains a backlog larger than one batch`() {
        val handled = AtomicInteger()
        val done = CountDownLatch(1)
        val poll = backlog(1_000).pollContinuously(cursor, wakeups.asFlux(), pollInterval = 1.hours, count = batchSize) {
            onSuccess { batch ->
                handled.addAndGet(batch.size)
                cursor.set(batch.keys.last())
                if (handled.get() == 1_000) done.countDown()
            }
            onFailure { throw AssertionError(it) }
        }
        try {
            assertTrue(done.await(5, TimeUnit.SECONDS), "handled ${handled.get()} of 1000 entries")
            settle()
        } finally {
            poll.dispose()
        }

        // Five full batches, then one empty read that ends the chain.
        assertEquals(6, reads.get())
        assertEquals(StreamMessageId(1_000, 0), cursor.get())
    }

    @Test
    fun `a full batch the handler does not consume is read once per trigger`() {
        val poll = backlog(1_000).pollContinuously(cursor, wakeups.asFlux(), pollInterval = 1.hours, count = batchSize) {
            onFailure { throw AssertionError(it) }
        }
        try {
            settle()
            assertEquals(1, reads.get())

            wakeups.tryEmitNext(Unit)
            settle()
            assertEquals(2, reads.get())
        } finally {
            poll.dispose()
        }
    }

    @Test
    fun `the drain stops once polling is suspended`() {
        val suspended = AtomicBoolean()
        val poll = backlog(1_000).pollContinuously(
            cursor, wakeups.asFlux(), pollInterval = 1.hours, count = batchSize, shouldPoll = { !suspended.get() },
        ) {
            onSuccess { batch ->
                cursor.set(batch.keys.last())
                suspended.set(true)
            }
            onFailure { throw AssertionError(it) }
        }
        try {
            settle()
        } finally {
            poll.dispose()
        }

        assertEquals(1, reads.get())
        assertEquals(StreamMessageId(200, 0), cursor.get())
    }

    @Test
    fun `a failed read ends the drain until the next trigger`() {
        val failures = AtomicInteger()
        val poll = backlog(1_000, failAt = 2).pollContinuously(
            cursor, wakeups.asFlux(), pollInterval = 1.hours, count = batchSize,
        ) {
            onSuccess { batch -> cursor.set(batch.keys.last()) }
            onFailure { failures.incrementAndGet() }
        }
        try {
            settle()
            assertEquals(2, reads.get())
            assertEquals(1, failures.get())

            wakeups.tryEmitNext(Unit)
            settle()
        } finally {
            poll.dispose()
        }

        // The wakeup resumes after the failed read and drains the remaining four batches plus one empty read.
        assertEquals(7, reads.get())
        assertEquals(StreamMessageId(1_000, 0), cursor.get())
    }
}
