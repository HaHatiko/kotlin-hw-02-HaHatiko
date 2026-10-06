package org.spbu.app

import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals

class StreamingJsonParserIntermediateNodesTest {
    private val json = Json

    @Test
    fun `default config emits intermediate nodes`() = runTest {
        val events = flowOf("{\"a\":1}")
            .parseStreamingJson()
            .toList()

        assertEquals(
            listOf(
                JsonEvent.NodeComplete(path("a"), json.parseToJsonElement("1")),
                JsonEvent.DocumentComplete(json.parseToJsonElement("{\"a\":1}")),
            ),
            events,
        )
    }

    @Test
    fun `emits intermediate nodes in post-order`() = runTest {
        val events = flowOf("{\"a\":[1,{\"b\":true}],\"c\":null}")
            .parseStreamingJson(StreamingJsonParserConfig(emitIntermediateNodes = true))
            .toList()

        assertEquals(
            listOf(
                JsonEvent.NodeComplete(path("a", 0), json.parseToJsonElement("1")),
                JsonEvent.NodeComplete(path("a", 1, "b"), json.parseToJsonElement("true")),
                JsonEvent.NodeComplete(path("a", 1), json.parseToJsonElement("{\"b\":true}")),
                JsonEvent.NodeComplete(path("a"), json.parseToJsonElement("[1,{\"b\":true}]")),
                JsonEvent.NodeComplete(path("c"), json.parseToJsonElement("null")),
                JsonEvent.DocumentComplete(json.parseToJsonElement("{\"a\":[1,{\"b\":true}],\"c\":null}")),
            ),
            events,
        )
    }

    @Test
    fun `emits primitive leaves for simple containers`() = runTest {
        val objectEvents = flowOf("{\"a\":1}")
            .parseStreamingJson(StreamingJsonParserConfig(emitIntermediateNodes = true))
            .toList()
        val arrayEvents = flowOf("[true]")
            .parseStreamingJson(StreamingJsonParserConfig(emitIntermediateNodes = true))
            .toList()

        assertEquals(
            listOf(
                JsonEvent.NodeComplete(path("a"), json.parseToJsonElement("1")),
                JsonEvent.DocumentComplete(json.parseToJsonElement("{\"a\":1}")),
            ),
            objectEvents,
        )
        assertEquals(
            listOf(
                JsonEvent.NodeComplete(path(0), json.parseToJsonElement("true")),
                JsonEvent.DocumentComplete(json.parseToJsonElement("[true]")),
            ),
            arrayEvents,
        )
    }

    @Test
    fun `does not duplicate root primitive as node complete`() = runTest {
        val events = flowOf("true")
            .parseStreamingJson(StreamingJsonParserConfig(emitIntermediateNodes = true))
            .toList()

        assertEquals(
            listOf(JsonEvent.DocumentComplete(json.parseToJsonElement("true"))),
            events,
        )
    }

    @Test
    fun `does not duplicate root container as node complete`() = runTest {
        val events = flowOf("[1]")
            .parseStreamingJson(StreamingJsonParserConfig(emitIntermediateNodes = true))
            .toList()

        assertEquals(
            listOf(
                JsonEvent.NodeComplete(path(0), json.parseToJsonElement("1")),
                JsonEvent.DocumentComplete(json.parseToJsonElement("[1]")),
            ),
            events,
        )
    }

    private fun path(vararg components: Any): JsonPath = components.fold(JsonPath.Root as JsonPath) { parent, component ->
        when (component) {
            is String -> JsonPath.Field(parent, component)
            is Int -> JsonPath.Index(parent, component)
            else -> error("Unsupported path component: $component")
        }
    }
}
