package org.spbu.app

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalCoroutinesApi::class)
class RateLimiterTest {
    private val clientA = ClientId("client-a")
    private val clientB = ClientId("client-b")
    private val clientC = ClientId("client-c")

    @Test
    fun `accepts requests within per-client limit`() = runTest {
        val limiter = limiter(perClientLimit = 2)
        var calls = 0

        val first = limiter.execute(ctx(clientA, "1")) {
            calls += 1
            "first"
        }
        val second = limiter.execute(ctx(clientA, "2")) {
            calls += 1
            "second"
        }

        assertEquals("first", first)
        assertEquals("second", second)
        assertEquals(2, calls)
    }

    @Test
    fun `rejects third request in the same sliding window`() = runTest {
        val time = ManualTimeProvider()
        val limiter = limiter(perClientLimit = 2, timeProvider = time)
        var thirdCalled = false

        limiter.execute(ctx(clientA, "1")) { "ok-1" }
        time.advanceBy(100)
        limiter.execute(ctx(clientA, "2")) { "ok-2" }
        time.advanceBy(100)

        val error = assertFailsWith<RateLimitRejected> {
            limiter.execute(ctx(clientA, "3")) {
                thirdCalled = true
                "should not run"
            }
        }

        assertEquals(clientA, error.clientId)
        assertEquals(800, error.retryAfterMillis)
        assertFalse(thirdCalled)
    }

    @Test
    fun `sliding window expires request exactly on boundary`() = runTest {
        val time = ManualTimeProvider()
        val limiter = limiter(perClientLimit = 2, timeProvider = time)

        limiter.execute(ctx(clientA, "1")) { "ok-1" }
        time.advanceBy(500)
        limiter.execute(ctx(clientA, "2")) { "ok-2" }
        time.advanceBy(499)
        assertFailsWith<RateLimitRejected> {
            limiter.execute(ctx(clientA, "3")) { "too early" }
        }

        time.advanceBy(1)
        val acceptedAtBoundary = limiter.execute(ctx(clientA, "4")) { "accepted" }

        assertEquals("accepted", acceptedAtBoundary)
    }

    @Test
    fun `limits are isolated per client`() = runTest {
        val limiter = limiter(perClientLimit = 1)

        val firstA = limiter.execute(ctx(clientA, "a-1")) { "a-1" }
        assertFailsWith<RateLimitRejected> {
            limiter.execute(ctx(clientA, "a-2")) { "a-2" }
        }
        val firstB = limiter.execute(ctx(clientB, "b-1")) { "b-1" }

        assertEquals("a-1", firstA)
        assertEquals("b-1", firstB)
    }

    @Test
    fun `global concurrency suspends instead of rejecting`() = runTest {
        val limiter = limiter(perClientLimit = 10, globalConcurrency = 1)
        val firstEntered = CompletableDeferred<Unit>()
        val releaseFirst = CompletableDeferred<Unit>()
        var secondEntered = false

        val first = launch {
            limiter.execute(ctx(clientA, "1")) {
                firstEntered.complete(Unit)
                releaseFirst.await()
                "first"
            }
        }
        firstEntered.await()

        val second = async {
            limiter.execute(ctx(clientB, "2")) {
                secondEntered = true
                "second"
            }
        }
        runCurrent()

        assertFalse(secondEntered)
        assertTrue(second.isActive)

        releaseFirst.complete(Unit)
        advanceUntilIdle()

        assertTrue(secondEntered)
        assertEquals("second", second.await())
        first.join()
    }

    @Test
    fun `global concurrency is shared across clients`() = runTest {
        val limiter = limiter(perClientLimit = 10, globalConcurrency = 2)
        val firstEntered = CompletableDeferred<Unit>()
        val secondEntered = CompletableDeferred<Unit>()
        val releaseOne = CompletableDeferred<Unit>()
        var active = 0
        var maxActive = 0
        var thirdEntered = false

        val first = launch {
            limiter.execute(ctx(clientA, "1")) {
                active += 1
                maxActive = maxOf(maxActive, active)
                firstEntered.complete(Unit)
                releaseOne.await()
                active -= 1
            }
        }
        val second = launch {
            limiter.execute(ctx(clientB, "2")) {
                active += 1
                maxActive = maxOf(maxActive, active)
                secondEntered.complete(Unit)
                releaseOne.await()
                active -= 1
            }
        }
        firstEntered.await()
        secondEntered.await()

        val third = async {
            limiter.execute(ctx(clientC, "3")) {
                thirdEntered = true
                active += 1
                maxActive = maxOf(maxActive, active)
                active -= 1
                "third"
            }
        }
        runCurrent()

        assertFalse(thirdEntered)
        releaseOne.complete(Unit)
        advanceUntilIdle()

        assertEquals("third", third.await())
        assertTrue(thirdEntered)
        assertEquals(2, maxActive)
        first.join()
        second.join()
    }

    @Test
    fun `rejected request does not wait for global semaphore`() = runTest {
        val limiter = limiter(perClientLimit = 1, globalConcurrency = 1)
        val holderEntered = CompletableDeferred<Unit>()
        val releaseHolder = CompletableDeferred<Unit>()
        var rejectedBlockCalled = false

        val holder = launch {
            limiter.execute(ctx(clientA, "1")) {
                holderEntered.complete(Unit)
                releaseHolder.await()
            }
        }
        holderEntered.await()

        val error = withTimeout(100.milliseconds) {
            assertFailsWith<RateLimitRejected> {
                limiter.execute(ctx(clientA, "2")) {
                    rejectedBlockCalled = true
                }
            }
        }

        assertEquals(clientA, error.clientId)
        assertFalse(rejectedBlockCalled)
        releaseHolder.complete(Unit)
        holder.join()
    }

    @Test
    fun `permit is released when block fails`() = runTest {
        val limiter = limiter(perClientLimit = 10, globalConcurrency = 1)
        val failure = IllegalStateException("boom")

        val thrown = assertFailsWith<IllegalStateException> {
            limiter.execute(ctx(clientA, "1")) {
                throw failure
            }
        }
        val second = withTimeout(1.seconds) {
            limiter.execute(ctx(clientB, "2")) { "ok" }
        }

        assertEquals(failure, thrown)
        assertEquals("ok", second)
    }

    @Test
    fun `permit is released on cancellation inside block`() = runTest {
        val limiter = limiter(perClientLimit = 10, globalConcurrency = 1)
        val entered = CompletableDeferred<Unit>()

        val first = launch {
            limiter.execute(ctx(clientA, "1")) {
                entered.complete(Unit)
                awaitCancellation()
            }
        }
        entered.await()
        first.cancelAndJoin()

        val second = withTimeout(1.seconds) {
            limiter.execute(ctx(clientB, "2")) { "ok" }
        }

        assertEquals("ok", second)
    }

    @Test
    fun `cancellation while waiting for global semaphore does not leak permit`() = runTest {
        val limiter = limiter(perClientLimit = 10, globalConcurrency = 1)
        val firstEntered = CompletableDeferred<Unit>()
        val releaseFirst = CompletableDeferred<Unit>()
        var secondEntered = false

        val first = launch {
            limiter.execute(ctx(clientA, "1")) {
                firstEntered.complete(Unit)
                releaseFirst.await()
            }
        }
        firstEntered.await()

        val second = launch {
            limiter.execute(ctx(clientB, "2")) {
                secondEntered = true
            }
        }
        runCurrent()
        assertFalse(secondEntered)
        second.cancelAndJoin()

        releaseFirst.complete(Unit)
        first.join()

        val third = withTimeout(1.seconds) {
            limiter.execute(ctx(clientC, "3")) { "ok" }
        }

        assertEquals("ok", third)
    }

    @Test
    fun `concurrent same-client requests cannot pass above limit`() = runTest {
        val limiter = limiter(perClientLimit = 1, globalConcurrency = 10)
        var executed = 0

        val first = async {
            runCatching {
                limiter.execute(ctx(clientA, "1")) {
                    executed += 1
                    "first"
                }
            }
        }
        val second = async {
            runCatching {
                limiter.execute(ctx(clientA, "2")) {
                    executed += 1
                    "second"
                }
            }
        }

        val results = listOf(first.await(), second.await())
        val successes = results.count { it.isSuccess }
        val rejections = results.mapNotNull { it.exceptionOrNull() }.filterIsInstance<RateLimitRejected>()

        assertEquals(1, successes)
        assertEquals(1, rejections.size)
        assertEquals(1, executed)
    }

    @Test
    fun `invalid config is rejected`() {
        assertFailsWith<IllegalArgumentException> {
            limiter(perClientLimit = 0)
        }
        assertFailsWith<IllegalArgumentException> {
            limiter(perClientWindow = 0.milliseconds)
        }
        assertFailsWith<IllegalArgumentException> {
            limiter(globalConcurrency = 0)
        }
    }

    @Test
    fun `cancellation exception from block is not converted to domain error`() = runTest {
        val limiter = limiter(perClientLimit = 10, globalConcurrency = 1)

        val thrown = assertFailsWith<CancellationException> {
            limiter.execute(ctx(clientA, "1")) {
                throw CancellationException("cancelled")
            }
        }

        assertIs<CancellationException>(thrown)
    }

    private fun limiter(
        perClientLimit: Int = 2,
        perClientWindow: kotlin.time.Duration = 1.seconds,
        globalConcurrency: Int = 10,
        timeProvider: TimeProvider = ManualTimeProvider(),
    ): RateLimiter = RateLimiter(
        config = RateLimiterConfig(
            perClientLimit = perClientLimit,
            perClientWindow = perClientWindow,
            globalConcurrency = globalConcurrency,
        ),
        timeProvider = timeProvider,
    )

    private fun ctx(clientId: ClientId, requestId: String): RequestContext = RequestContext(
        clientId = clientId,
        requestId = RequestId(requestId),
    )
}

private class ManualTimeProvider(startMillis: Long = 0L) : TimeProvider {
    var now: Long = startMillis
        private set

    override fun nowMillis(): Long = now

    fun advanceBy(millis: Long) {
        now += millis
    }
}
