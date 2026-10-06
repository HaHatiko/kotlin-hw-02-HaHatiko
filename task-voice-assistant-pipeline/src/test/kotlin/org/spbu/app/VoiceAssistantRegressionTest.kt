package org.spbu.app

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlin.coroutines.ContinuationInterceptor
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds

@OptIn(ExperimentalCoroutinesApi::class)
class VoiceAssistantRegressionTest {
    private val ctx = RequestContext(UserId("u"), RequestId("r"))
    private val request = AudioRequest(byteArrayOf(1))

    @Test
    fun `all six external operations run on the supplied dispatcher`() = runTest {
        val fixture = Fixture(StandardTestDispatcher(testScheduler))
        val assistant = fixture.assistant()
        try {
            assistant.process(ctx, request)
            advanceUntilIdle()
            assertEquals(setOf("stt", "load", "generate", "tts", "extract", "save"), fixture.calls.toSet())
        } finally {
            assistant.close()
        }
    }

    @Test
    fun `fact extraction starts while the main response is still running`() = runTest {
        val fixture = Fixture(StandardTestDispatcher(testScheduler))
        val extractionStarted = CompletableDeferred<Unit>()
        val releaseSpeech = CompletableDeferred<Unit>()
        fixture.onExtract = {
            extractionStarted.complete(Unit)
            awaitCancellation()
        }
        fixture.onSpeech = { releaseSpeech.await() }
        val assistant = fixture.assistant()
        try {
            val response = async { assistant.process(ctx, request) }
            runCurrent()
            assertTrue(extractionStarted.isCompleted)
            assertFalse(response.isCompleted)
            releaseSpeech.complete(Unit)
            assertEquals("response", response.await().responseText)
        } finally {
            assistant.close()
        }
    }

    @Test
    fun `outer timeout is propagated without retrying or domain wrapping`() = runTest {
        val fixture = Fixture(StandardTestDispatcher(testScheduler))
        fixture.onTranscribe = { awaitCancellation() }
        val assistant = fixture.assistant(VoiceAssistantConfig(speechToTextRetries = 3))
        try {
            assertFailsWith<CancellationException> {
                withTimeout(10.milliseconds) {
                    assistant.process(ctx, request)
                }
            }
            assertEquals(listOf("stt"), fixture.calls)
        } finally {
            assistant.close()
        }
    }

    @Test
    fun `optional service cancellation is not converted to an empty list`() = runTest {
        val fixture = Fixture(StandardTestDispatcher(testScheduler))
        val cancelled = CancellationException("load cancelled")
        fixture.onLoad = { throw cancelled }
        val assistant = fixture.assistant(VoiceAssistantConfig(loadFactsRetries = 3))
        try {
            val actual = assertFailsWith<CancellationException> { assistant.process(ctx, request) }
            // kotlinx.coroutines может скопировать исключение для восстановления stack trace.
            assertEquals(cancelled.message, actual.message)
            assertEquals(1, fixture.calls.count { it == "load" })
            assertFalse("generate" in fixture.calls)
        } finally {
            assistant.close()
        }
    }

    @Test
    fun `independent facts can be saved in parallel`() = runTest {
        val fixture = Fixture(StandardTestDispatcher(testScheduler))
        fixture.facts = listOf(UserFact("a"), UserFact("b"))
        val started = mutableListOf<UserFact>()
        val releaseSaves = CompletableDeferred<Unit>()
        fixture.onSave = { fact ->
            started.add(fact)
            releaseSaves.await()
        }
        val assistant = fixture.assistant()
        try {
            assistant.process(ctx, request)
            runCurrent()
            assertEquals(fixture.facts, started)
            releaseSaves.complete(Unit)
            advanceUntilIdle()
        } finally {
            assistant.close()
        }
    }

    private class Fixture(private val dispatcher: CoroutineDispatcher) {
        val calls = mutableListOf<String>()
        var facts = listOf(UserFact("fact"))
        var onTranscribe: suspend () -> Unit = {}
        var onLoad: suspend () -> Unit = {}
        var onExtract: suspend () -> Unit = {}
        var onSpeech: suspend () -> Unit = {}
        var onSave: suspend (UserFact) -> Unit = {}

        private suspend fun record(name: String) {
            assertSame(dispatcher, currentCoroutineContext()[ContinuationInterceptor])
            calls.add(name)
        }

        fun assistant(config: VoiceAssistantConfig = VoiceAssistantConfig()): VoiceAssistant {
            return VoiceAssistant(
                speechToText = object : SpeechToTextService {
                    override suspend fun transcribe(ctx: RequestContext, audio: ByteArray): String {
                        record("stt")
                        onTranscribe()
                        return "text"
                    }
                },
                userFacts = object : UserFactsService {
                    override suspend fun loadFacts(ctx: RequestContext, text: String): List<UserFact> {
                        record("load")
                        onLoad()
                        return emptyList()
                    }
                    override suspend fun saveFact(ctx: RequestContext, fact: UserFact) {
                        record("save")
                        onSave(fact)
                    }
                },
                llm = object : LlmService {
                    override suspend fun generateResponse(ctx: RequestContext, text: String, facts: List<UserFact>): String {
                        record("generate")
                        return "response"
                    }
                    override suspend fun extractFacts(ctx: RequestContext, text: String): List<UserFact> {
                        record("extract")
                        onExtract()
                        return facts
                    }
                },
                textToSpeech = object : TextToSpeechService {
                    override suspend fun synthesize(ctx: RequestContext, text: String, format: AudioFormat): ByteArray {
                        record("tts")
                        onSpeech()
                        return byteArrayOf(2)
                    }
                },
                config = config,
                ioDispatcher = dispatcher,
            )
        }
    }
}
