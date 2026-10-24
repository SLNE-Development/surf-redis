package dev.slne.surf.redis.sync

import reactor.core.publisher.Sinks
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SyncStreamWakeupEmitTest {

    // Same spec as SyncStreamWakeupBus.register
    private val sink = Sinks.many().multicast().directBestEffort<Unit>()

    private val attempts = AtomicInteger()
    private val firstCollision = CountDownLatch(1)

    private val countingSink = object : Sinks.Many<Unit> by sink {
        override fun tryEmitNext(t: Unit): Sinks.EmitResult {
            attempts.incrementAndGet()
            return sink.tryEmitNext(t).also {
                if (it == Sinks.EmitResult.FAIL_NON_SERIALIZED) firstCollision.countDown()
            }
        }
    }

    private val received = AtomicInteger()
    private val holderInside = CountDownLatch(1)
    private val releaseHolder = CountDownLatch(1)
    private var holder: Thread? = null

    @AfterTest
    fun releaseHeldEmission() {
        releaseHolder.countDown()
        holder?.join(5_000)
    }

    /** Parks another thread inside `onNext` of the first wakeup, holding the sink's serialization guard. */
    private fun holdEmission() {
        sink.asFlux().subscribe {
            if (received.incrementAndGet() == 1) {
                holderInside.countDown()
                releaseHolder.await()
            }
        }
        holder = thread(name = "wakeup-holder") { sink.tryEmitNext(Unit) }
        assertTrue(holderInside.await(5, TimeUnit.SECONDS))
    }

    @Test
    fun `a raw emission colliding with another thread's wakeup is rejected`() {
        holdEmission()

        assertEquals(Sinks.EmitResult.FAIL_NON_SERIALIZED, sink.tryEmitNext(Unit))

        releaseHolder.countDown()
        holder!!.join(5_000)
        assertEquals(1, received.get())
    }

    @Test
    fun `a wakeup colliding with another thread's wakeup is still delivered`() {
        holdEmission()

        val emitter = thread(name = "wakeup-emitter") {
            countingSink.emitWakeup(collisionBudgetNanos = TimeUnit.SECONDS.toNanos(10))
        }
        assertTrue(firstCollision.await(5, TimeUnit.SECONDS))
        releaseHolder.countDown()

        emitter.join(5_000)
        assertEquals(false, emitter.isAlive)
        assertEquals(2, received.get())
    }

    @Test
    fun `a collision outlasting the budget gives up instead of blocking the caller`() {
        holdEmission()

        val start = System.nanoTime()
        countingSink.emitWakeup(collisionBudgetNanos = TimeUnit.MILLISECONDS.toNanos(20))
        val elapsed = System.nanoTime() - start

        assertTrue(elapsed < TimeUnit.SECONDS.toNanos(2), "emitWakeup blocked for ${elapsed / 1_000_000} ms")
        assertTrue(attempts.get() > 1)
        assertEquals(1, received.get())
    }

    @Test
    fun `wakeups without a subscriber or after unregister are dropped without retry`() {
        countingSink.emitWakeup()
        assertEquals(1, attempts.get())

        sink.tryEmitComplete()
        countingSink.emitWakeup()
        assertEquals(2, attempts.get())
    }
}
