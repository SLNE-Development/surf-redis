package dev.slne.surf.redis.sync

import reactor.core.publisher.Mono
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals

class SyncKeyEventBusTest {

    private val handlers = mutableMapOf<String, (String) -> Unit>()
    private val subscriptions = AtomicInteger()

    private val bus = SyncKeyEventBus { pattern, onKey ->
        Mono.fromCallable {
            subscriptions.incrementAndGet()
            handlers[pattern] = onKey
        }
    }

    private fun fire(pattern: String, key: String) = handlers.getValue(pattern)(key)

    @Test
    fun `del and expired reach only the callbacks of the affected key`() {
        bus.init().block()
        val a = AtomicInteger()
        val b = AtomicInteger()
        bus.register("a") { a.incrementAndGet() }
        bus.register("b") { b.incrementAndGet() }

        fire(SyncKeyEventBus.DELETED_PATTERN, "a")
        fire(SyncKeyEventBus.EXPIRED_PATTERN, "a")
        fire(SyncKeyEventBus.DELETED_PATTERN, "unrelated")

        assertEquals(2, a.get())
        assertEquals(0, b.get())
    }

    @Test
    fun `an unregistered callback no longer fires while others on the same key still do`() {
        bus.init().block()
        val first = AtomicInteger()
        val second = AtomicInteger()
        val firstCallback = Runnable { first.incrementAndGet() }
        bus.register("key", firstCallback)
        bus.register("key") { second.incrementAndGet() }

        bus.unregister("key", firstCallback)
        fire(SyncKeyEventBus.DELETED_PATTERN, "key")

        assertEquals(0, first.get())
        assertEquals(1, second.get())
    }

    @Test
    fun `a failing callback does not suppress the others`() {
        bus.init().block()
        val reached = AtomicInteger()
        bus.register("key") { error("boom") }
        bus.register("key") { reached.incrementAndGet() }

        fire(SyncKeyEventBus.DELETED_PATTERN, "key")

        assertEquals(1, reached.get())
    }

    @Test
    fun `each pattern is subscribed once however many structures initialize`() {
        repeat(3) { bus.init().block() }

        assertEquals(2, subscriptions.get())
        assertEquals(
            setOf(SyncKeyEventBus.DELETED_PATTERN, SyncKeyEventBus.EXPIRED_PATTERN),
            handlers.keys,
        )
    }
}
