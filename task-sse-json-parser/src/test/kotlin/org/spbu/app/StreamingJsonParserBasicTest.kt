package org.spbu.app

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalCoroutinesApi::class)
class StreamingJsonParserBasicTest {
    private val json = Json

    @Test
    fun `parses complete JSON from one plain fragment`() = runTest {
        val events = flowOf("{\"a\":1}")
            .parseStreamingJson(StreamingJsonParserConfig(emitIntermediateNodes = false))
            .toList()

        assertEquals(
            listOf(JsonEvent.DocumentComplete(json.parseToJsonElement("{\"a\":1}"))),
            events,
        )
    }

    @Test
    fun `parses JSON split across plain fragments`() = runTest {
        val events = flowOf(
            "{\"a\"",
            ":[1,",
            "2]}",
        ).parseStreamingJson(StreamingJsonParserConfig(emitIntermediateNodes = false)).toList()

        assertEquals(
            JsonEvent.DocumentComplete(json.parseToJsonElement("{\"a\":[1,2]}")),
            events.single(),
        )
    }

    @Test
    fun `does not emit intermediate nodes when disabled`() = runTest {
        val events = flowOf("{\"a\":[1,{\"b\":true}],\"c\":null}")
            .parseStreamingJson(StreamingJsonParserConfig(emitIntermediateNodes = false))
            .toList()

        assertEquals(
            listOf(JsonEvent.DocumentComplete(json.parseToJsonElement("{\"a\":[1,{\"b\":true}],\"c\":null}"))),
            events,
        )
    }

    @Test
    fun `first fragment timeout`() = runTest {
        val config = StreamingJsonParserConfig(
            firstTokenTimeout = 100.milliseconds,
            emitIntermediateNodes = false,
        )
        val events = emptyFlow<String>()
            .onStart { delay(10.seconds) }
            .parseStreamingJson(config)

        val deferred = async { assertFailsWith<FirstTokenTimeoutException> { events.toList() } }
        advanceTimeBy(100.milliseconds)
        runCurrent()

        assertEquals(100.milliseconds, deferred.await().timeout)
    }

    @Test
    fun `next fragment timeout while JSON is incomplete`() = runTest {
        val config = StreamingJsonParserConfig(
            nextTokenTimeout = 100.milliseconds,
            emitIntermediateNodes = false,
        )
        val events = flow {
            emit("{\"a\":")
            delay(10.seconds)
        }.parseStreamingJson(config)

        val deferred = async { assertFailsWith<NextTokenTimeoutException> { events.toList() } }
        runCurrent()
        advanceTimeBy(100.milliseconds)
        runCurrent()

        assertEquals(100.milliseconds, deferred.await().timeout)
    }

    @Test
    fun `no timeout after complete document`() = runTest {
        val config = StreamingJsonParserConfig(
            nextTokenTimeout = 100.milliseconds,
            emitIntermediateNodes = false,
        )
        val events = flow {
            emit("{\"a\":1}")
            delay(Long.MAX_VALUE)
        }.parseStreamingJson(config).toList()

        assertEquals(
            JsonEvent.DocumentComplete(json.parseToJsonElement("{\"a\":1}")),
            events.single(),
        )
    }

    @Test
    fun `size limit exceeded`() = runTest {
        val config = StreamingJsonParserConfig(
            maxJsonSizeBytes = 8,
            emitIntermediateNodes = false,
        )

        val error = assertFailsWith<JsonSizeLimitExceededException> {
            flowOf("{\"long\":\"abcdef\"}")
                .parseStreamingJson(config)
                .toList()
        }

        assertEquals(8, error.maxJsonSizeBytes)
    }

    @Test
    fun `incomplete JSON on upstream completion`() = runTest {
        assertFailsWith<IncompleteJsonException> {
            flowOf("{\"a\": 1")
                .parseStreamingJson(StreamingJsonParserConfig(emitIntermediateNodes = false))
                .toList()
        }
    }

    @Test
    fun `invalid JSON token`() = runTest {
        assertFailsWith<InvalidJsonTokenException> {
            flowOf("{\"a\":}")
                .parseStreamingJson(StreamingJsonParserConfig(emitIntermediateNodes = false))
                .toList()
        }
    }

    @Test
    fun `invalid escape sequence`() = runTest {
        assertFailsWith<InvalidJsonTokenException> {
            flowOf("{\"s\":\"\\x\"}")
                .parseStreamingJson(StreamingJsonParserConfig(emitIntermediateNodes = false))
                .toList()
        }
    }

    @Test
    fun `unicode escape split across fragments`() = runTest {
        val events = flowOf(
            "{\"s\":\"\\u00",
            "41\"}",
        ).parseStreamingJson(StreamingJsonParserConfig(emitIntermediateNodes = false)).toList()

        assertEquals(
            JsonEvent.DocumentComplete(json.parseToJsonElement("{\"s\":\"A\"}")),
            events.single(),
        )
    }

    @Test
    fun `multiple JSON values rejected`() = runTest {
        assertFailsWith<MultipleJsonValuesException> {
            flowOf("{\"a\":1} [2]")
                .parseStreamingJson(StreamingJsonParserConfig(emitIntermediateNodes = false))
                .toList()
        }
    }

    @Test
    fun `multiple JSON values across fragments rejected`() = runTest {
        assertFailsWith<MultipleJsonValuesException> {
            flowOf("{\"a\":1}", "[2]")
                .parseStreamingJson(StreamingJsonParserConfig(emitIntermediateNodes = false))
                .toList()
        }
    }

    @Test
    fun `upstream exception is propagated`() = runTest {
        val upstreamError = IllegalStateException("boom")

        val error = assertFailsWith<IllegalStateException> {
            flow<String> { throw upstreamError }
                .parseStreamingJson(StreamingJsonParserConfig(emitIntermediateNodes = false))
                .toList()
        }

        assertSame(upstreamError, error)
    }

    @Test
    fun `cancellation while waiting for next fragment is not converted to timeout`() = runTest {
        val config = StreamingJsonParserConfig(
            nextTokenTimeout = 10.seconds,
            emitIntermediateNodes = false,
        )
        val started = kotlinx.coroutines.CompletableDeferred<Unit>()
        val job = launch {
            assertFailsWith<CancellationException> {
                flow {
                    emit("{\"a\":")
                    started.complete(Unit)
                    awaitCancellation()
                }.parseStreamingJson(config).toList()
            }
        }

        started.await()
        job.cancelAndJoin()

        assertTrue(job.isCancelled)
    }

    @Test
    fun `every upstream element is treated as JSON content`() = runTest {
        val validEvents = flowOf("{\"ok\":true}")
            .parseStreamingJson(StreamingJsonParserConfig(emitIntermediateNodes = false))
            .toList()

        assertIs<JsonEvent.DocumentComplete>(validEvents.single())
        assertFailsWith<InvalidJsonTokenException> {
            flowOf("event: message", "{\"ok\":true}")
                .parseStreamingJson(StreamingJsonParserConfig(emitIntermediateNodes = false))
                .toList()
        }
    }
}
