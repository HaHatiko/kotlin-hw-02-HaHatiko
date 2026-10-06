package org.spbu.app

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.yield
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

data class StreamingJsonParserConfig(
    val firstTokenTimeout: Duration = 1.seconds,
    val nextTokenTimeout: Duration = 500.milliseconds,
    val maxJsonSizeBytes: Int = 64 * 1024,
    val emitIntermediateNodes: Boolean = true,
) {
    init {
        require(firstTokenTimeout > Duration.ZERO) { "firstTokenTimeout must be positive" }
        require(nextTokenTimeout > Duration.ZERO) { "nextTokenTimeout must be positive" }
        require(maxJsonSizeBytes > 0) { "maxJsonSizeBytes must be positive" }
    }
}

sealed interface JsonEvent {
    val path: JsonPath
    val element: JsonElement

    data class NodeComplete(
        override val path: JsonPath,
        override val element: JsonElement,
    ) : JsonEvent

    data class DocumentComplete(
        override val element: JsonElement,
    ) : JsonEvent {
        override val path: JsonPath = JsonPath.Root
    }
}

sealed interface JsonPath {
    data object Root : JsonPath
    data class Field(val parent: JsonPath, val name: String) : JsonPath
    data class Index(val parent: JsonPath, val index: Int) : JsonPath
}

sealed class StreamingJsonParserException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

class FirstTokenTimeoutException(val timeout: Duration) : StreamingJsonParserException("No first token within $timeout")
class NextTokenTimeoutException(val timeout: Duration) : StreamingJsonParserException("No next token within $timeout")
class JsonSizeLimitExceededException(val maxJsonSizeBytes: Int) : StreamingJsonParserException("JSON size limit exceeded: $maxJsonSizeBytes bytes")
class InvalidJsonTokenException(val offset: Int, cause: Throwable? = null) : StreamingJsonParserException("Invalid JSON token at offset $offset", cause)
class IncompleteJsonException(val offset: Int) : StreamingJsonParserException("Input ended before JSON value was complete at offset $offset")
class MultipleJsonValuesException(val offset: Int) : StreamingJsonParserException("Multiple JSON values are not allowed at offset $offset")

fun Flow<String>.parseStreamingJson(
    config: StreamingJsonParserConfig = StreamingJsonParserConfig(),
): Flow<JsonEvent> {
    val upstream = this
    return flow {
        coroutineScope {
            val fragments = Channel<String>(capacity = 1)
            val producer = launch {
                try {
                    upstream.collect { fragment -> fragments.send(fragment) }
                } catch (error: Throwable) {
                    fragments.close(error)
                } finally {
                    fragments.close()
                }
            }

            try {
                val input = JsonFragmentReader(fragments, config)
                val parser = JsonDocumentParser(input, config, this@flow)
                parser.parseDocument()
            } finally {
                producer.cancel()
                fragments.cancel()
            }
        }
    }
}

private class JsonFragmentReader(
    private val fragments: ReceiveChannel<String>,
    private val config: StreamingJsonParserConfig,
) {
    private var fragment = ""
    private var position = 0
    private var bytesReceived = 0L
    private var firstFragment = true
    private var finished = false

    var offset = 0
        private set

    suspend fun peek(): Char? {
        while (position == fragment.length) {
            if (finished) {
                return null
            }
            receiveFragment()
        }
        return fragment[position]
    }

    suspend fun read(): Char {
        val character = peek()
        if (character == null) {
            throw IncompleteJsonException(offset)
        }
        position++
        offset++
        if (offset % 256 == 0) {
            yield()
        }
        return character
    }

    private suspend fun receiveFragment() {
        currentCoroutineContext().ensureActive()
        val timeout = if (firstFragment) config.firstTokenTimeout else config.nextTokenTimeout
        val received = withTimeoutOrNull(timeout) {
            fragments.receiveCatching()
        }
        if (received == null) {
            if (firstFragment) {
                throw FirstTokenTimeoutException(timeout)
            }
            throw NextTokenTimeoutException(timeout)
        }

        if (received.isClosed) {
            val error = received.exceptionOrNull()
            if (error != null) {
                throw error
            }
            finished = true
            return
        }

        firstFragment = false
        acceptFragment(received.getOrThrow())
    }

    private fun acceptFragment(value: String) {
        bytesReceived += value.toByteArray(Charsets.UTF_8).size
        if (bytesReceived > config.maxJsonSizeBytes) {
            throw JsonSizeLimitExceededException(config.maxJsonSizeBytes)
        }
        fragment = value
        position = 0
    }

    suspend fun skipWhitespace() {
        while (isJsonWhitespace(peek())) {
            read()
        }
    }

    suspend fun checkTrailingContent() {
        checkRemainingFragment()
        while (true) {
            yield()
            val received = fragments.tryReceive()
            if (received.isFailure) {
                val error = received.exceptionOrNull()
                if (error != null) {
                    throw error
                }
                return
            }
            acceptFragment(received.getOrThrow())
            checkRemainingFragment()
        }
    }

    private suspend fun checkRemainingFragment() {
        while (position < fragment.length) {
            if (!isJsonWhitespace(fragment[position])) {
                throw MultipleJsonValuesException(offset)
            }
            read()
        }
    }
}

private fun isJsonWhitespace(character: Char?): Boolean {
    return character == ' ' || character == '\t' || character == '\r' || character == '\n'
}

private class JsonDocumentParser(
    private val input: JsonFragmentReader,
    private val config: StreamingJsonParserConfig,
    private val events: FlowCollector<JsonEvent>,
) {
    suspend fun parseDocument() {
        val root = readValue(JsonPath.Root)
        input.checkTrailingContent()
        events.emit(JsonEvent.DocumentComplete(root))
    }

    private suspend fun readValue(path: JsonPath): JsonElement {
        currentCoroutineContext().ensureActive()
        input.skipWhitespace()
        val character = input.peek()
        val element = when (character) {
            '{' -> readObject(path)
            '[' -> readArray(path)
            '"' -> JsonPrimitive(readString())
            't' -> readLiteral("true", JsonPrimitive(true))
            'f' -> readLiteral("false", JsonPrimitive(false))
            'n' -> readLiteral("null", JsonNull)
            '-', in '0'..'9' -> readNumber()
            null -> throw IncompleteJsonException(input.offset)
            else -> throw InvalidJsonTokenException(input.offset)
        }

        if (config.emitIntermediateNodes && path != JsonPath.Root) {
            events.emit(JsonEvent.NodeComplete(path, element))
        }
        return element
    }

    private suspend fun readObject(path: JsonPath): JsonObject {
        expect('{')
        val fields = linkedMapOf<String, JsonElement>()
        input.skipWhitespace()
        if (input.peek() == '}') {
            input.read()
            return JsonObject(fields)
        }

        while (true) {
            input.skipWhitespace()
            val name = readString()
            input.skipWhitespace()
            expect(':')
            fields[name] = readValue(JsonPath.Field(path, name))
            input.skipWhitespace()
            when (input.read()) {
                '}' -> return JsonObject(fields)
                ',' -> Unit
                else -> throw InvalidJsonTokenException(input.offset - 1)
            }
        }
    }

    private suspend fun readArray(path: JsonPath): JsonArray {
        expect('[')
        val values = mutableListOf<JsonElement>()
        input.skipWhitespace()
        if (input.peek() == ']') {
            input.read()
            return JsonArray(values)
        }

        while (true) {
            val childPath = JsonPath.Index(path, values.size)
            values.add(readValue(childPath))
            input.skipWhitespace()
            when (input.read()) {
                ']' -> return JsonArray(values)
                ',' -> Unit
                else -> throw InvalidJsonTokenException(input.offset - 1)
            }
        }
    }

    private suspend fun readString(): String {
        expect('"')
        val text = StringBuilder()
        while (true) {
            val character = input.read()
            when {
                character == '"' -> return text.toString()
                character == '\\' -> text.append(readEscape())
                character < ' ' -> throw InvalidJsonTokenException(input.offset - 1)
                else -> text.append(character)
            }
        }
    }

    private suspend fun readEscape(): Char {
        return when (input.read()) {
            '"' -> '"'
            '\\' -> '\\'
            '/' -> '/'
            'b' -> '\b'
            'f' -> '\u000C'
            'n' -> '\n'
            'r' -> '\r'
            't' -> '\t'
            'u' -> readUnicodeEscape()
            else -> throw InvalidJsonTokenException(input.offset - 1)
        }
    }

    private suspend fun readUnicodeEscape(): Char {
        var code = 0
        repeat(4) {
            val character = input.read()
            val digit = when (character) {
                in '0'..'9' -> character - '0'
                in 'a'..'f' -> character - 'a' + 10
                in 'A'..'F' -> character - 'A' + 10
                else -> throw InvalidJsonTokenException(input.offset - 1)
            }
            code = code * 16 + digit
        }
        return code.toChar()
    }

    private suspend fun readLiteral(text: String, value: JsonElement): JsonElement {
        for (character in text) {
            expect(character)
        }
        return value
    }

    private suspend fun readNumber(): JsonElement {
        val number = StringBuilder()
        if (input.peek() == '-') {
            number.append(input.read())
        }

        if (input.peek() == '0') {
            number.append(input.read())
            if (isDigit(input.peek())) {
                throw InvalidJsonTokenException(input.offset)
            }
        } else {
            readDigits(number)
        }

        if (input.peek() == '.') {
            number.append(input.read())
            readDigits(number)
        }
        if (input.peek() == 'e' || input.peek() == 'E') {
            number.append(input.read())
            if (input.peek() == '+' || input.peek() == '-') {
                number.append(input.read())
            }
            readDigits(number)
        }

        val next = input.peek()
        if (next != null && !isJsonWhitespace(next) && next != ',' && next != ']' && next != '}') {
            throw InvalidJsonTokenException(input.offset)
        }
        return Json.parseToJsonElement(number.toString())
    }

    private suspend fun readDigits(number: StringBuilder) {
        val first = input.peek()
        if (first == null) {
            throw IncompleteJsonException(input.offset)
        }
        if (!isDigit(first)) {
            throw InvalidJsonTokenException(input.offset)
        }
        while (isDigit(input.peek())) {
            number.append(input.read())
        }
    }

    private fun isDigit(character: Char?): Boolean {
        return character != null && character in '0'..'9'
    }

    private suspend fun expect(expected: Char) {
        val actual = input.read()
        if (actual != expected) {
            throw InvalidJsonTokenException(input.offset - 1)
        }
    }
}
