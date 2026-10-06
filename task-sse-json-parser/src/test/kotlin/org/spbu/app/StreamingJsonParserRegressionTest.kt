package org.spbu.app

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

@OptIn(ExperimentalCoroutinesApi::class)
class StreamingJsonParserRegressionTest {
    private val documentOnly = StreamingJsonParserConfig(emitIntermediateNodes = false)

    @Test
    fun `valid values work at every possible fragment boundary`() = runTest {
        val documents = listOf(
            "null", "true", "false", "0", "-0", "-123.45e-6", "1E+20",
            "[]", "{}", "\"hello\"", "\"\\u0041\\n\\t\\b\\f\\r\\/\\\\\\\"\"",
            " {\"a\":[1,null,false,{\"b\":\"Привет 😺\"}],\"c\":-2.5e+3} \n",
        )
        for (document in documents) {
            for (boundary in 0..document.length) {
                val events = flowOf(document.take(boundary), document.drop(boundary))
                    .parseStreamingJson(documentOnly).toList()
                assertEquals(Json.parseToJsonElement(document), events.single().element, document)
            }
        }
    }

    @Test
    fun `one character fragments preserve numbers literals and strings`() = runTest {
        val document = "{\"a\":[-1.23e+4,true,false,null,\"a\\u0041b\"]}"
        val fragments = document.map { it.toString() }.toTypedArray()
        val result = flowOf(*fragments).parseStreamingJson(documentOnly).toList()
        assertEquals(Json.parseToJsonElement(document), result.single().element)
    }

    @Test
    fun `invalid grammar is rejected without accepting lenient JSON`() = runTest {
        val invalid = listOf(
            "[1,]", "{\"a\":1,}", "{a:1}", "[1 2]", "{\"a\" 1}",
            "[01]", "[-01]", "[1.]", "[1e+]", "[+1]", "[NaN]", "[truX]",
            "\"\\u00xz\"", "\"line\nbreak\"", "\u00a0{}",
        )
        for (document in invalid) {
            assertFailsWith<InvalidJsonTokenException>(document) {
                flowOf(document).parseStreamingJson(documentOnly).toList()
            }
        }
    }

    @Test
    fun `unfinished grammar is distinguished from invalid grammar`() = runTest {
        val incomplete = listOf("", " ", "{", "[", "\"abc", "\"\\u00", "tru", "-", "1.", "1e+", "{\"a\":", "[1,")
        for (document in incomplete) {
            assertFailsWith<IncompleteJsonException>(document) {
                flowOf(document).parseStreamingJson(documentOnly).toList()
            }
        }
        assertFailsWith<IncompleteJsonException> {
            emptyFlow<String>().parseStreamingJson(documentOnly).toList()
        }
    }

    @Test
    fun `byte limit counts UTF8 bytes and accepts exact boundary`() = runTest {
        val document = "\"я😺\""
        val bytes = document.toByteArray(Charsets.UTF_8).size
        val result = flowOf(document).parseStreamingJson(documentOnly.copy(maxJsonSizeBytes = bytes)).toList()
        assertEquals(Json.parseToJsonElement(document), result.single().element)
        assertFailsWith<JsonSizeLimitExceededException> {
            flowOf(document).parseStreamingJson(documentOnly.copy(maxJsonSizeBytes = bytes - 1)).toList()
        }
    }

    @Test
    fun `each collection starts a new parser and reads upstream again`() = runTest {
        var starts = 0
        val parsed = flow {
            starts++
            emit("{\"a\":")
            emit("1}")
        }.parseStreamingJson()
        assertEquals(0, starts)
        assertEquals(parsed.toList(), parsed.toList())
        assertEquals(2, starts)
    }

    @Test
    fun `nested nodes are emitted before root completion`() = runTest {
        val continueDocument = CompletableDeferred<Unit>()
        val events = mutableListOf<JsonEvent>()
        val job = launch {
            flow {
                emit("{\"a\":[1],")
                continueDocument.await()
                emit("\"b\":true}")
            }.parseStreamingJson().collect { events.add(it) }
        }
        runCurrent()
        assertEquals(2, events.size)
        assertTrue(events.all { it is JsonEvent.NodeComplete })
        assertEquals(JsonPath.Field(JsonPath.Root, "a"), events.last().path)
        continueDocument.complete(Unit)
        job.join()
        assertIs<JsonEvent.DocumentComplete>(events.last())
    }

    @Test
    fun `collector errors are not wrapped as parser errors`() = runTest {
        val failure = IllegalArgumentException("collector failed")
        val actual = assertFailsWith<IllegalArgumentException> {
            flowOf("{\"a\":1}").parseStreamingJson().collect { throw failure }
        }
        assertSame(failure, actual)
    }

    @Test
    fun `collector cancellation and take clean up the upstream`() = runTest {
        var stopped = false
        val events = flow {
            try {
                emit("[1,")
                awaitCancellation()
            } finally {
                stopped = true
            }
        }.parseStreamingJson().take(1).toList()
        assertEquals(1, events.size)
        assertTrue(stopped)

        val cancellation = CancellationException("collector stopped")
        val actual = assertFailsWith<CancellationException> {
            flowOf("{}").parseStreamingJson().collect { throw cancellation }
        }
        assertSame(cancellation, actual)
    }

    @Test
    fun `upstream failure after partial content keeps its identity`() = runTest {
        val failure = IllegalStateException("upstream failed")
        val actual = assertFailsWith<IllegalStateException> {
            flow {
                emit("{\"a\":")
                throw failure
            }.parseStreamingJson().toList()
        }
        assertSame(failure, actual)
    }

    @Test
    fun `upstream cancellation keeps its identity`() = runTest {
        val failure = CancellationException("upstream cancelled")
        val actual = assertFailsWith<CancellationException> {
            flow<String> { throw failure }.parseStreamingJson().toList()
        }
        assertSame(failure, actual)
    }

    @Test
    fun `parent timeout is not converted to fragment timeout`() = runTest {
        assertFailsWith<CancellationException> {
            withTimeout(10.milliseconds) {
                flow<String> { awaitCancellation() }.parseStreamingJson().toList()
            }
        }
    }

    @Test
    fun `complete string literal and delimited number do not wait for hot upstream`() = runTest {
        for (document in listOf("\"hello\"", "true", "false", "null", "42 ")) {
            var stopped = false
            val events = flow {
                try {
                    emit(document)
                    awaitCancellation()
                } finally {
                    stopped = true
                }
            }.parseStreamingJson(documentOnly).toList()
            assertEquals(Json.parseToJsonElement(document), events.single().element)
            assertTrue(stopped)
        }
    }

    @Test
    fun `deep nesting does not overflow the JVM stack`() = runTest {
        val depth = 1500
        val document = "[".repeat(depth) + "true" + "]".repeat(depth)
        var element = flowOf(document).parseStreamingJson(documentOnly).toList().single().element
        repeat(depth) {
            element = assertIs<JsonArray>(element).single()
        }
        assertEquals(Json.parseToJsonElement("true"), element)
    }

    @Test
    fun `second root after several ready whitespace fragments is rejected`() = runTest {
        assertFailsWith<MultipleJsonValuesException> {
            flowOf("{}", " ", "", "\n", " ", "[]")
                .parseStreamingJson(documentOnly).toList()
        }
    }

    @Test
    fun `trailing whitespace fragments are allowed`() = runTest {
        val events = flowOf("{}", " ", "", "\n", "\t")
            .parseStreamingJson(documentOnly).toList()
        assertEquals(Json.parseToJsonElement("{}"), events.single().element)
    }

    @Test
    fun `invalid configuration is rejected`() {
        assertFailsWith<IllegalArgumentException> { StreamingJsonParserConfig(firstTokenTimeout = Duration.ZERO) }
        assertFailsWith<IllegalArgumentException> { StreamingJsonParserConfig(nextTokenTimeout = (-1).milliseconds) }
        assertFailsWith<IllegalArgumentException> { StreamingJsonParserConfig(maxJsonSizeBytes = 0) }
    }
}
