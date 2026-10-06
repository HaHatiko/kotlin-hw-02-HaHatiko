package org.spbu.app

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestDispatcher
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class VoiceAssistantTest {
    private val ctx = RequestContext(UserId("user-1"), RequestId("request-1"))
    private val request = AudioRequest(byteArrayOf(1, 2, 3), AudioFormat.MP3)

    @Test
    fun `successful pipeline passes data through services and returns synthesized response`() = runTest {
        val fakes = VoiceAssistantFakes()
        val assistant = fakes.assistant(StandardTestDispatcher(testScheduler))

        val response = assistant.process(ctx, request)
        advanceUntilIdle()

        assertContentEquals(fakes.audioResponse, response.audio)
        assertEquals(request.format, response.format)
        assertEquals(fakes.responseText, response.responseText)
        assertContentEquals(request.audio, fakes.speechToText.receivedAudio.single())
        assertEquals(listOf(fakes.transcribedText), fakes.userFacts.loadTexts)
        assertEquals(listOf(fakes.loadedFacts), fakes.llm.generateFacts)
        assertEquals(listOf(fakes.responseText), fakes.textToSpeech.synthesizeTexts)
        assertEquals(listOf(request.format), fakes.textToSpeech.synthesizeFormats)
    }

    @Test
    fun `load facts failure uses degraded mode and empty facts`() = runTest {
        val fakes = VoiceAssistantFakes()
        fakes.userFacts.loadFailure = IllegalStateException("facts are unavailable")
        val assistant = fakes.assistant(StandardTestDispatcher(testScheduler))

        val response = assistant.process(ctx, request)
        advanceUntilIdle()

        assertEquals(fakes.responseText, response.responseText)
        assertEquals(listOf(emptyList()), fakes.llm.generateFacts)
    }

    @Test
    fun `speech to text failure is fatal and stops pipeline`() = runTest {
        val fakes = VoiceAssistantFakes()
        fakes.speechToText.failure = IllegalStateException("stt failed")
        val assistant = fakes.assistant(StandardTestDispatcher(testScheduler))

        assertFailsWith<SpeechToTextFailed> {
            assistant.process(ctx, request)
        }

        assertEquals(1, fakes.speechToText.calls)
        assertEquals(0, fakes.userFacts.loadCalls)
        assertEquals(0, fakes.llm.generateCalls)
        assertEquals(0, fakes.textToSpeech.calls)
    }

    @Test
    fun `llm response failure is fatal and skips text to speech`() = runTest {
        val fakes = VoiceAssistantFakes()
        fakes.llm.generateFailure = IllegalStateException("llm failed")
        val assistant = fakes.assistant(StandardTestDispatcher(testScheduler))

        assertFailsWith<ResponseGenerationFailed> {
            assistant.process(ctx, request)
        }

        assertEquals(1, fakes.speechToText.calls)
        assertEquals(1, fakes.userFacts.loadCalls)
        assertEquals(1, fakes.llm.generateCalls)
        assertEquals(0, fakes.textToSpeech.calls)
    }

    @Test
    fun `text to speech failure is fatal`() = runTest {
        val fakes = VoiceAssistantFakes()
        fakes.textToSpeech.failure = IllegalStateException("tts failed")
        val assistant = fakes.assistant(StandardTestDispatcher(testScheduler))

        assertFailsWith<TextToSpeechFailed> {
            assistant.process(ctx, request)
        }

        assertEquals(1, fakes.textToSpeech.calls)
    }

    @Test
    fun `background fact extraction and saving do not block response`() = runTest {
        val extractStarted = CompletableDeferred<Unit>()
        val allowExtractFinish = CompletableDeferred<Unit>()
        val fakes = VoiceAssistantFakes()
        fakes.llm.onExtractFacts = { _, _ ->
            extractStarted.complete(Unit)
            allowExtractFinish.await()
            listOf(UserFact("likes Kotlin"))
        }
        val assistant = fakes.assistant(StandardTestDispatcher(testScheduler))

        val response = assistant.process(ctx, request)

        assertContentEquals(fakes.audioResponse, response.audio)
        assertTrue(extractStarted.isCompleted)
        assertEquals(emptyList(), fakes.userFacts.savedFacts)

        allowExtractFinish.complete(Unit)
        advanceUntilIdle()
        assertEquals(listOf(UserFact("likes Kotlin")), fakes.userFacts.savedFacts)
    }

    @Test
    fun `save fact retries failed fact and still attempts other facts`() = runTest {
        val fakes = VoiceAssistantFakes()
        fakes.llm.extractedFacts = listOf(UserFact("bad"), UserFact("good"))
        fakes.userFacts.saveFailuresByFact[UserFact("bad")] = 1
        val assistant = fakes.assistant(
            dispatcher = StandardTestDispatcher(testScheduler),
            config = VoiceAssistantConfig(saveFactRetries = 1),
        )

        assistant.process(ctx, request)
        advanceUntilIdle()

        assertEquals(
            listOf(UserFact("bad"), UserFact("bad"), UserFact("good")),
            fakes.userFacts.saveAttempts,
        )
        assertEquals(listOf(UserFact("bad"), UserFact("good")), fakes.userFacts.savedFacts)
    }

    @Test
    fun `mandatory service retries ordinary failures before succeeding`() = runTest {
        val fakes = VoiceAssistantFakes()
        fakes.speechToText.failuresBeforeSuccess = 2
        val assistant = fakes.assistant(
            dispatcher = StandardTestDispatcher(testScheduler),
            config = VoiceAssistantConfig(speechToTextRetries = 2),
        )

        val response = assistant.process(ctx, request)

        assertEquals(fakes.responseText, response.responseText)
        assertEquals(3, fakes.speechToText.calls)
    }

    @Test
    fun `speech to text timeout is retried then mapped to domain failure`() = runTest {
        val fakes = VoiceAssistantFakes()
        fakes.speechToText.onTranscribe = { _, _ -> delay(1.seconds) }
        val assistant = fakes.assistant(
            dispatcher = StandardTestDispatcher(testScheduler),
            config = VoiceAssistantConfig(
                speechToTextTimeout = 100.milliseconds,
                speechToTextRetries = 2,
            ),
        )

        assertFailsWith<SpeechToTextFailed> {
            assistant.process(ctx, request)
        }
        assertEquals(3, fakes.speechToText.calls)
        assertEquals(0, fakes.userFacts.loadCalls)
    }

    @Test
    fun `load facts timeout is retried then degraded to empty facts`() = runTest {
        val fakes = VoiceAssistantFakes()
        fakes.userFacts.onLoadFacts = { _, _ -> delay(1.seconds) }
        val assistant = fakes.assistant(
            dispatcher = StandardTestDispatcher(testScheduler),
            config = VoiceAssistantConfig(
                loadFactsTimeout = 100.milliseconds,
                loadFactsRetries = 1,
            ),
        )

        val response = assistant.process(ctx, request)

        assertEquals(fakes.responseText, response.responseText)
        assertEquals(2, fakes.userFacts.loadCalls)
        assertEquals(listOf(emptyList()), fakes.llm.generateFacts)
    }

    @Test
    fun `llm response timeout is retried then mapped to domain failure`() = runTest {
        val fakes = VoiceAssistantFakes()
        fakes.llm.onGenerateResponse = { _, _, _ -> delay(1.seconds) }
        val assistant = fakes.assistant(
            dispatcher = StandardTestDispatcher(testScheduler),
            config = VoiceAssistantConfig(
                generateResponseTimeout = 100.milliseconds,
                generateResponseRetries = 1,
            ),
        )

        assertFailsWith<ResponseGenerationFailed> {
            assistant.process(ctx, request)
        }
        assertEquals(2, fakes.llm.generateCalls)
        assertEquals(0, fakes.textToSpeech.calls)
    }

    @Test
    fun `text to speech timeout is retried then mapped to domain failure`() = runTest {
        val fakes = VoiceAssistantFakes()
        fakes.textToSpeech.onSynthesize = { _, _, _ -> delay(1.seconds) }
        val assistant = fakes.assistant(
            dispatcher = StandardTestDispatcher(testScheduler),
            config = VoiceAssistantConfig(
                textToSpeechTimeout = 100.milliseconds,
                textToSpeechRetries = 1,
            ),
        )

        assertFailsWith<TextToSpeechFailed> {
            assistant.process(ctx, request)
        }
        assertEquals(2, fakes.textToSpeech.calls)
    }

    @Test
    fun `background extract facts timeout is retried and does not fail response`() = runTest {
        val fakes = VoiceAssistantFakes()
        fakes.llm.onExtractFacts = { _, _ ->
            delay(1.seconds)
            emptyList()
        }
        val assistant = fakes.assistant(
            dispatcher = StandardTestDispatcher(testScheduler),
            config = VoiceAssistantConfig(
                extractFactsTimeout = 100.milliseconds,
                extractFactsRetries = 1,
            ),
        )

        val response = assistant.process(ctx, request)
        advanceUntilIdle()

        assertEquals(fakes.responseText, response.responseText)
        assertEquals(2, fakes.llm.extractCalls)
        assertEquals(emptyList(), fakes.userFacts.saveAttempts)
    }

    @Test
    fun `background save fact timeout is retried and does not fail response`() = runTest {
        val fakes = VoiceAssistantFakes()
        val fact = UserFact("slow fact")
        fakes.llm.extractedFacts = listOf(fact)
        fakes.userFacts.onSaveFact = { _, _ -> delay(1.seconds) }
        val assistant = fakes.assistant(
            dispatcher = StandardTestDispatcher(testScheduler),
            config = VoiceAssistantConfig(
                saveFactTimeout = 100.milliseconds,
                saveFactRetries = 1,
            ),
        )

        val response = assistant.process(ctx, request)
        advanceUntilIdle()

        assertEquals(fakes.responseText, response.responseText)
        assertEquals(listOf(fact, fact), fakes.userFacts.saveAttempts)
        assertEquals(emptyList(), fakes.userFacts.savedFacts)
    }

    @Test
    fun `cancellation exception is not retried or wrapped`() = runTest {
        val fakes = VoiceAssistantFakes()
        fakes.speechToText.failure = CancellationException("cancelled")
        val assistant = fakes.assistant(
            dispatcher = StandardTestDispatcher(testScheduler),
            config = VoiceAssistantConfig(speechToTextRetries = 3),
        )

        assertFailsWith<CancellationException> {
            assistant.process(ctx, request)
        }
        assertEquals(1, fakes.speechToText.calls)
    }

    @Test
    fun `close forbids new requests and cancels background work`() = runTest {
        val backgroundCancelled = CompletableDeferred<Unit>()
        val extractStarted = CompletableDeferred<Unit>()
        val fakes = VoiceAssistantFakes()
        fakes.llm.onExtractFacts = { _, _ ->
            extractStarted.complete(Unit)
            try {
                awaitCancellation()
            } finally {
                backgroundCancelled.complete(Unit)
            }
        }
        val assistant = fakes.assistant(StandardTestDispatcher(testScheduler))

        val processJob = launch { assistant.process(ctx, request) }
        advanceUntilIdle()
        processJob.cancelAndJoin()
        assertTrue(extractStarted.isCompleted)

        assistant.close()
        advanceUntilIdle()

        assertTrue(backgroundCancelled.isCompleted)
        assertFailsWith<VoiceAssistantClosed> {
            assistant.process(ctx, request)
        }
    }

    private class VoiceAssistantFakes {
        val transcribedText = "hello assistant"
        val responseText = "hello user"
        val audioResponse = byteArrayOf(9, 8, 7)
        val loadedFacts = listOf(UserFact("uses Kotlin"))

        val speechToText = FakeSpeechToText(transcribedText)
        val userFacts = FakeUserFacts(loadedFacts)
        val llm = FakeLlm(responseText)
        val textToSpeech = FakeTextToSpeech(audioResponse)

        fun assistant(
            dispatcher: TestDispatcher,
            config: VoiceAssistantConfig = VoiceAssistantConfig(
                speechToTextTimeout = 5.seconds,
                loadFactsTimeout = 5.seconds,
                generateResponseTimeout = 5.seconds,
                textToSpeechTimeout = 5.seconds,
                extractFactsTimeout = 5.seconds,
                saveFactTimeout = 5.seconds,
            ),
        ) = VoiceAssistant(speechToText, userFacts, llm, textToSpeech, config, dispatcher)
    }

    private class FakeSpeechToText(private val result: String) : SpeechToTextService {
        var calls = 0
        var failure: Throwable? = null
        var failuresBeforeSuccess = 0
        var onTranscribe: (suspend (RequestContext, ByteArray) -> Unit)? = null
        val receivedAudio = mutableListOf<ByteArray>()

        override suspend fun transcribe(ctx: RequestContext, audio: ByteArray): String {
            calls++
            receivedAudio += audio
            onTranscribe?.invoke(ctx, audio)
            failure?.let { throw it }
            if (calls <= failuresBeforeSuccess) {
                throw IllegalStateException("temporary stt failure")
            }
            return result
        }
    }

    private class FakeUserFacts(private val facts: List<UserFact>) : UserFactsService {
        var loadCalls = 0
        var loadFailure: Throwable? = null
        var onLoadFacts: (suspend (RequestContext, String) -> Unit)? = null
        var onSaveFact: (suspend (RequestContext, UserFact) -> Unit)? = null
        val loadTexts = mutableListOf<String>()
        val saveAttempts = mutableListOf<UserFact>()
        val savedFacts = mutableListOf<UserFact>()
        val saveFailuresByFact = mutableMapOf<UserFact, Int>()

        override suspend fun loadFacts(ctx: RequestContext, text: String): List<UserFact> {
            loadCalls++
            loadTexts += text
            onLoadFacts?.invoke(ctx, text)
            loadFailure?.let { throw it }
            return facts
        }

        override suspend fun saveFact(ctx: RequestContext, fact: UserFact) {
            saveAttempts += fact
            onSaveFact?.invoke(ctx, fact)
            val remainingFailures = saveFailuresByFact[fact] ?: 0
            if (remainingFailures > 0) {
                saveFailuresByFact[fact] = remainingFailures - 1
                throw IllegalStateException("temporary save failure")
            }
            savedFacts += fact
        }
    }

    private class FakeLlm(private val result: String) : LlmService {
        var generateCalls = 0
        var extractCalls = 0
        var generateFailure: Throwable? = null
        var extractedFacts = listOf(UserFact("new fact"))
        var onGenerateResponse: (suspend (RequestContext, String, List<UserFact>) -> Unit)? = null
        var onExtractFacts: (suspend (RequestContext, String) -> List<UserFact>)? = null
        val generateTexts = mutableListOf<String>()
        val generateFacts = mutableListOf<List<UserFact>>()

        override suspend fun generateResponse(
            ctx: RequestContext,
            text: String,
            facts: List<UserFact>,
        ): String {
            generateCalls++
            generateTexts += text
            generateFacts += facts
            onGenerateResponse?.invoke(ctx, text, facts)
            generateFailure?.let { throw it }
            return result
        }

        override suspend fun extractFacts(ctx: RequestContext, text: String): List<UserFact> {
            extractCalls++
            onExtractFacts?.let { return it(ctx, text) }
            return extractedFacts
        }
    }

    private class FakeTextToSpeech(private val result: ByteArray) : TextToSpeechService {
        var calls = 0
        var failure: Throwable? = null
        var onSynthesize: (suspend (RequestContext, String, AudioFormat) -> Unit)? = null
        val synthesizeTexts = mutableListOf<String>()
        val synthesizeFormats = mutableListOf<AudioFormat>()

        override suspend fun synthesize(ctx: RequestContext, text: String, format: AudioFormat): ByteArray {
            calls++
            synthesizeTexts += text
            synthesizeFormats += format
            onSynthesize?.invoke(ctx, text, format)
            failure?.let { throw it }
            return result
        }
    }
}
