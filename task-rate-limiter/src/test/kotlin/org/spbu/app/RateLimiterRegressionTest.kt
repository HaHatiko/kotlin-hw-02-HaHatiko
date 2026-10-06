package org.spbu.app

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.microseconds
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalCoroutinesApi::class)
class RateLimiterRegressionTest {
    @Test
    fun `cancelled accepted request still occupies its client window`() = runTest {
        val limiter = RateLimiter(RateLimiterConfig(1, 10.seconds, 1), object : TimeProvider {
            override fun nowMillis() = 0L
        })
        val holderEntered = CompletableDeferred<Unit>()
        val holder = launch {
            limiter.execute(RequestContext(ClientId("a"), RequestId("1"))) {
                holderEntered.complete(Unit)
                awaitCancellation()
            }
        }
        holderEntered.await()
        val waiting = launch {
            limiter.execute(RequestContext(ClientId("b"), RequestId("2"))) { error("must not execute") }
        }
        runCurrent()
        waiting.cancelAndJoin()
        holder.cancelAndJoin()
        assertFailsWith<RateLimitRejected> {
            limiter.execute(RequestContext(ClientId("b"), RequestId("3"))) { Unit }
        }
    }

    @Test
    fun `positive fractional millisecond window is not rounded down to zero`() = runTest {
        var now = 0L
        val limiter = RateLimiter(RateLimiterConfig(1, 500.microseconds, 1), object : TimeProvider {
            override fun nowMillis() = now
        })
        val ctx = RequestContext(ClientId("a"), RequestId("1"))
        limiter.execute(ctx) { Unit }
        val rejected = assertFailsWith<RateLimitRejected> { limiter.execute(ctx) { Unit } }
        assertEquals(1L, rejected.retryAfterMillis)
        now = 1L
        limiter.execute(ctx) { Unit }
    }
}
