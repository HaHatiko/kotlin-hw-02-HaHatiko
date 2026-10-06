package org.spbu.app

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@JvmInline
value class UserId(val value: String)

@JvmInline
value class RequestId(val value: String)

data class RequestContext(
    val userId: UserId,
    val requestId: RequestId,
)

data class AudioRequest(
    val audio: ByteArray,
    val format: AudioFormat = AudioFormat.WAV,
)

data class AudioResponse(
    val audio: ByteArray,
    val format: AudioFormat = AudioFormat.WAV,
    val responseText: String,
)

enum class AudioFormat {
    WAV, MP3
}

data class UserFact(val value: String)

data class VoiceAssistantConfig(
    val speechToTextTimeout: Duration = 1.seconds,
    val loadFactsTimeout: Duration = 1.seconds,
    val generateResponseTimeout: Duration = 2.seconds,
    val textToSpeechTimeout: Duration = 1.seconds,
    val extractFactsTimeout: Duration = 2.seconds,
    val saveFactTimeout: Duration = 500.milliseconds,
    val speechToTextRetries: Int = 0,
    val loadFactsRetries: Int = 0,
    val generateResponseRetries: Int = 0,
    val textToSpeechRetries: Int = 0,
    val extractFactsRetries: Int = 0,
    val saveFactRetries: Int = 0,
)

interface SpeechToTextService {
    suspend fun transcribe(ctx: RequestContext, audio: ByteArray): String
}

interface UserFactsService {
    suspend fun loadFacts(ctx: RequestContext, text: String): List<UserFact>
    suspend fun saveFact(ctx: RequestContext, fact: UserFact)
}

interface LlmService {
    suspend fun generateResponse(ctx: RequestContext, text: String, facts: List<UserFact>): String
    suspend fun extractFacts(ctx: RequestContext, text: String): List<UserFact>
}

interface TextToSpeechService {
    suspend fun synthesize(ctx: RequestContext, text: String, format: AudioFormat): ByteArray
}

sealed class VoiceAssistantException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

class SpeechToTextFailed(cause: Throwable? = null) : VoiceAssistantException("Speech-to-text failed", cause)
class ResponseGenerationFailed(cause: Throwable? = null) : VoiceAssistantException("LLM response generation failed", cause)
class TextToSpeechFailed(cause: Throwable? = null) : VoiceAssistantException("Text-to-speech failed", cause)
class VoiceAssistantClosed : VoiceAssistantException("Voice assistant is closed")

class VoiceAssistant(
    private val speechToText: SpeechToTextService,
    private val userFacts: UserFactsService,
    private val llm: LlmService,
    private val textToSpeech: TextToSpeechService,
    private val config: VoiceAssistantConfig = VoiceAssistantConfig(),
    private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : AutoCloseable {
    private val backgroundScope = CoroutineScope(SupervisorJob() + ioDispatcher)

    @Volatile
    private var closed = false

    suspend fun process(ctx: RequestContext, request: AudioRequest): AudioResponse {
        if (closed) {
            throw VoiceAssistantClosed()
        }

        return coroutineScope {
            val text = callService(
                timeout = config.speechToTextTimeout,
                retries = config.speechToTextRetries,
                onFailure = { error -> throw SpeechToTextFailed(error) },
            ) {
                speechToText.transcribe(ctx, request.audio)
            }

            backgroundScope.launch {
                extractAndSaveFacts(ctx, text)
            }

            val facts = callService(
                timeout = config.loadFactsTimeout,
                retries = config.loadFactsRetries,
                onFailure = { emptyList<UserFact>() },
            ) {
                userFacts.loadFacts(ctx, text)
            }

            val responseText = callService(
                timeout = config.generateResponseTimeout,
                retries = config.generateResponseRetries,
                onFailure = { error -> throw ResponseGenerationFailed(error) },
            ) {
                llm.generateResponse(ctx, text, facts)
            }

            val audio = callService(
                timeout = config.textToSpeechTimeout,
                retries = config.textToSpeechRetries,
                onFailure = { error -> throw TextToSpeechFailed(error) },
            ) {
                textToSpeech.synthesize(ctx, responseText, request.format)
            }

            AudioResponse(audio, request.format, responseText)
        }
    }

    private suspend fun extractAndSaveFacts(ctx: RequestContext, text: String) {
        val facts = callService(
            timeout = config.extractFactsTimeout,
            retries = config.extractFactsRetries,
            onFailure = { emptyList<UserFact>() },
        ) {
            llm.extractFacts(ctx, text)
        }

        coroutineScope {
            val saves = mutableListOf<Deferred<Unit>>()
            for (fact in facts) {
                val save = async {
                    callService(
                        timeout = config.saveFactTimeout,
                        retries = config.saveFactRetries,
                        onFailure = { Unit },
                    ) {
                        userFacts.saveFact(ctx, fact)
                    }
                }
                saves.add(save)
            }
            for (save in saves) {
                save.await()
            }
        }
    }

    private suspend fun <T> callService(
        timeout: Duration,
        retries: Int,
        onFailure: (Exception) -> T,
        action: suspend () -> T,
    ): T {
        var retriesUsed = 0
        while (true) {
            currentCoroutineContext().ensureActive()
            try {
                return withContext(ioDispatcher) {
                    withTimeout(timeout) {
                        action()
                    }
                }
            } catch (error: Exception) {
                currentCoroutineContext().ensureActive()
                if (error is CancellationException && error !is TimeoutCancellationException) {
                    throw error
                }

                if (retriesUsed >= retries) {
                    return onFailure(error)
                }
                retriesUsed++
            }
        }
    }

    override fun close() {
        closed = true
        backgroundScope.cancel()
    }
}
