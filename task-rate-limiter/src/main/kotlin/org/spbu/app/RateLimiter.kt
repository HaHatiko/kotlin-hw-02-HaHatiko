package org.spbu.app

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

data class RateLimiterConfig(
    val perClientLimit: Int,
    val perClientWindow: Duration,
    val globalConcurrency: Int,
)

@JvmInline
value class ClientId(val value: String)

@JvmInline
value class RequestId(val value: String)

data class RequestContext(
    val clientId: ClientId,
    val requestId: RequestId,
)

interface TimeProvider {
    fun nowMillis(): Long
}

object SystemTimeProvider : TimeProvider {
    override fun nowMillis(): Long = System.currentTimeMillis()
}

class RateLimitRejected(
    val clientId: ClientId,
    val retryAfterMillis: Long,
) : RuntimeException("Rate limit exceeded for client ${clientId.value}; retry after $retryAfterMillis ms")

class RateLimiter(
    private val config: RateLimiterConfig,
    private val timeProvider: TimeProvider = SystemTimeProvider,
) {
    init {
        require(config.perClientLimit > 0) { "perClientLimit must be positive" }
        require(config.perClientWindow > Duration.ZERO) { "perClientWindow must be positive" }
        require(config.globalConcurrency > 0) { "globalConcurrency must be positive" }
    }

    private val clientLock = Mutex()
    private val clientRequests = mutableMapOf<ClientId, MutableList<Long>>()
    private val semaphore = Semaphore(config.globalConcurrency)

    suspend fun <T> execute(ctx: RequestContext, block: suspend () -> T): T {
        registerRequest(ctx.clientId)

        semaphore.acquire()
        try {
            return block()
        } finally {
            semaphore.release()
        }
    }

    private suspend fun registerRequest(clientId: ClientId) {
        clientLock.withLock {
            val now = timeProvider.nowMillis()
            val windowStart = now.milliseconds - config.perClientWindow
            var timestamps = clientRequests[clientId]
            if (timestamps == null) {
                timestamps = mutableListOf()
                clientRequests[clientId] = timestamps
            }

            timestamps.removeAll { timestamp -> timestamp.milliseconds <= windowStart }

            if (timestamps.size >= config.perClientLimit) {
                val oldest = timestamps.min()
                val remaining = oldest.milliseconds + config.perClientWindow - now.milliseconds
                var retryAfterMillis = remaining.inWholeMilliseconds
                if (retryAfterMillis.milliseconds < remaining) {
                    retryAfterMillis++
                }
                throw RateLimitRejected(clientId, retryAfterMillis)
            }

            timestamps.add(now)
        }
    }
}
