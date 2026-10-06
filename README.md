# hw-02-coroutines

## task-rate-limiter

**Задание обязательное. Оценка бинарная: 0 или 50 баллов.**

Реализуйте `RateLimiter` — coroutine-friendly компонент, который ограничивает количество запросов от каждого клиента по sliding window и общий параллелизм обработки запросов.

Для каждого клиента разрешено не более `perClientLimit` запросов за последние `perClientWindow`. Если лимит клиента превышен, запрос должен быть немедленно отклонён доменной ошибкой `RateLimitRejected`.

Для всех клиентов вместе одновременно может выполняться не более `globalConcurrency` запросов. Если глобальные слоты заняты, запрос не отклоняется, а приостанавливается до освобождения слота.

Ожидаемый публичный API:

```kotlin
class RateLimiter(
    private val config: RateLimiterConfig,
    private val timeProvider: TimeProvider = SystemTimeProvider,
) {
    suspend fun <T> execute(
        ctx: RequestContext,
        block: suspend () -> T,
    ): T
}
```

Общая последовательность для каждого запроса:

```text
execute(ctx, block)
  1. Проверить per-client sliding window под защитой общей критической секции.
  2. Если клиент превысил лимит — немедленно бросить RateLimitRejected.
  3. Если клиент разрешён — записать timestamp принятого запроса.
  4. Приостановиться на global Semaphore, если все глобальные слоты заняты.
  5. После получения глобального слота выполнить block.
  6. Всегда освободить глобальный слот в finally.
  7. Вернуть результат block или пробросить его исключение.
```

В окне остаются только запросы с timestamp `> now - perClientWindow`. Запрос ровно на границе окна считается устаревшим и больше не занимает слот.

Для ожидания глобального слота используйте coroutine-friendly механизм, например `kotlinx.coroutines.sync.Semaphore`. `Thread.sleep`, `java.util.concurrent.Semaphore.acquire()` и `runBlocking` внутри реализации запрещены.

Конструктор должен бросать `IllegalArgumentException`, если `perClientLimit <= 0`, `perClientWindow <= Duration.ZERO` или `globalConcurrency <= 0`.

Отмена корутины во время ожидания глобального слота или внутри `block` не должна приводить к утечке permit. `CancellationException` нельзя превращать в доменную ошибку. Исключения из `block` должны пробрасываться наружу как есть. Принятый per-client запрос считается попыткой и остаётся в sliding window даже если позже был отменён.

Для детерминированных тестов используйте переданный `TimeProvider`, а не прямой вызов `System.currentTimeMillis()` внутри алгоритма.

**Баллы: 0 или 50**

## task-voice-assistant-pipeline

**Задание обязательное. Оценка бинарная: 0 или 50 баллов.**

Реализуйте `VoiceAssistant` — lifecycle-компонент, который оркестрирует несколько внешних `suspend`-сервисов голосового ассистента.

Метод `process` принимает аудио пользователя, транскрибирует его через STT-сервис, загружает сохранённые факты о пользователе, генерирует текст ответа через LLM, синтезирует аудио через TTS и возвращает `AudioResponse`.

Основной pipeline:

```text
STT → load facts + LLM → TTS
```

После успешной транскрибации `VoiceAssistant` должен в фоне извлечь новые факты из текста пользователя через LLM и сохранить их в `UserFactsService`. Эта фоновая работа не должна задерживать возврат `AudioResponse`.

Ожидаемый публичный API:

```kotlin
class VoiceAssistant(
    private val speechToText: SpeechToTextService,
    private val userFacts: UserFactsService,
    private val llm: LlmService,
    private val textToSpeech: TextToSpeechService,
    private val config: VoiceAssistantConfig = VoiceAssistantConfig(),
    private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : AutoCloseable {
    suspend fun process(ctx: RequestContext, request: AudioRequest): AudioResponse

    override fun close()
}
```

Все внешние вызовы должны выполняться на переданном `ioDispatcher`, иметь timeout и использовать настраиваемое количество retries из `VoiceAssistantConfig`. Ошибки STT, генерации ответа LLM и TTS после исчерпания retries являются fatal и превращаются в доменные ошибки. Ошибка загрузки сохранённых фактов после исчерпания retries не fatal: ответ генерируется с пустым списком facts. Ошибки фонового извлечения или сохранения фактов после исчерпания retries не должны ломать основной ответ.

Используйте `coroutineScope` для обязательной части pipeline, `async` там, где есть осмысленный параллелизм, и управляемый background scope для фонового сохранения фактов. `GlobalScope`, `runBlocking` внутри реализации, `Thread.sleep` и блокирующие ожидания запрещены.

**Баллы: 0 или 50**

## task-sse-json-parser

**Задание бонусное. Оценка состоит из двух независимых частей: 0, 50 или 100 баллов.**

Реализуйте streaming parser для JSON, который приходит строковыми фрагментами через `Flow<String>`.

На вход подаётся поток фрагментов JSON-текста. Каждый элемент `Flow<String>` — это непосредственная часть JSON content, без SSE-префиксов, управляющих строк, event names или автоматических разделителей. Фрагменты конкатенируются без добавления `\n`. Например:

```kotlin
flowOf(
    "{\"a\"",
    ": ",
    "[1, 2]",
    "}",
)
```

соответствует JSON-документу:

```json
{"a": [1, 2]}
```

Это **не SSE-control parser**: строка вроде `event: message` должна рассматриваться как JSON content и приводить к ошибке JSON-грамматики, а не пропускаться как служебная строка.

Парсер должен собрать ровно один JSON value из стандартной JSON grammar: object, array, string, number, boolean или null. После завершения root value допускается только trailing whitespace; второй JSON value должен приводить к доменной ошибке. Partial fragments разрешены в строках, escape sequences, числах, литералах, массивах и объектах, если итоговая грамматика валидна.

Ожидаемый публичный API:

```kotlin
fun Flow<String>.parseStreamingJson(
    config: StreamingJsonParserConfig = StreamingJsonParserConfig(),
): Flow<JsonEvent>
```

`parseStreamingJson` возвращает cold `Flow<JsonEvent>`: парсинг начинается при `collect`, каждый collector заново читает upstream, исключения parser-а падают из returned flow при collection. Реализация не должна запускать неуправляемые фоновые корутины.

Конфигурация:

```kotlin
data class StreamingJsonParserConfig(
    val firstTokenTimeout: Duration = 1.seconds,
    val nextTokenTimeout: Duration = 500.milliseconds,
    val maxJsonSizeBytes: Int = 64 * 1024,
    val emitIntermediateNodes: Boolean = true,
)
```

`firstTokenTimeout` ограничивает ожидание первого fragment. `nextTokenTimeout` ограничивает ожидание следующего fragment, пока JSON value ещё не завершён. После `DocumentComplete` parser должен завершиться и не ждать hot upstream бесконечно. `maxJsonSizeBytes` считается по UTF-8 байтам входных fragments. Некорректный config (`timeout <= 0`, `maxJsonSizeBytes <= 0`) должен приводить к `IllegalArgumentException`.

События:

```kotlin
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
```

`DocumentComplete` — событие завершения единственного root JSON value и всегда последнее событие. Если `emitIntermediateNodes = false`, parser эмитит только `DocumentComplete`. Если `emitIntermediateNodes = true`, parser дополнительно эмитит `NodeComplete` для полностью завершённых вложенных узлов в **post-order**: сначала дочерние узлы, затем родитель. Root object/array не должен дублироваться как `NodeComplete($, ...)`.

Path к узлам задаётся иерархией:

```kotlin
sealed interface JsonPath {
    data object Root : JsonPath
    data class Field(val parent: JsonPath, val name: String) : JsonPath
    data class Index(val parent: JsonPath, val index: Int) : JsonPath
}
```

Пример порядка событий для JSON:

```json
{"a":[1,{"b":true}],"c":null}
```

```text
NodeComplete($.a[0], 1)
NodeComplete($.a[1].b, true)
NodeComplete($.a[1], {"b":true})
NodeComplete($.a, [1,{"b":true}])
NodeComplete($.c, null)
DocumentComplete({"a":[1,{"b":true}],"c":null})
```

Доменные ошибки parser-а должны быть наследниками sealed `StreamingJsonParserException`: `FirstTokenTimeoutException`, `NextTokenTimeoutException`, `JsonSizeLimitExceededException`, `InvalidJsonTokenException`, `IncompleteJsonException`, `MultipleJsonValuesException`. `CancellationException` нельзя заворачивать в parser exception; отмена collector-а или parent job должна пробрасываться как cancellation. Исключения upstream flow должны пробрасываться наружу без заворачивания.

Реализация должна быть coroutine-friendly: используйте suspend/Flow primitives, не используйте `GlobalScope`, `runBlocking`, `Thread.sleep`, blocking queues или блокирующие ожидания. Для длинного parsing loop периодически проверяйте cancellation.

**Баллы: 0, 50 или 100**

- первые 50 баллов: базовая часть `StreamingJsonParserBasicTest` — parser корректно собирает ровно один JSON value, обрабатывает split fragments, fragment timeouts, size limit, invalid/incomplete input, multiple values, cancellation/upstream exceptions и эмитит `DocumentComplete`; режим `emitIntermediateNodes = false` работает корректно;
- ещё 50 баллов: дополнительная часть `StreamingJsonParserIntermediateNodesTest` — при `emitIntermediateNodes = true` parser эмитит `NodeComplete` для завершённых вложенных узлов в post-order без дублирования root как `NodeComplete`, а затем финальный `DocumentComplete`.

Локально можно запускать части отдельно:

```bash
./gradlew --no-daemon :task-sse-json-parser:test --tests org.spbu.app.StreamingJsonParserBasicTest
./gradlew --no-daemon :task-sse-json-parser:test --tests org.spbu.app.StreamingJsonParserIntermediateNodesTest
```

## Оценивание

- `task-rate-limiter`: 0 или 50 баллов
- `task-voice-assistant-pipeline`: 0 или 50 баллов
- `task-sse-json-parser`: бонус, 0, 50 или 100 баллов
- Дедлайн: день выдачи + 2 недели
- После дедлайна: 50%

> Преподаватель может попросить исправить код, несмотря на проходящие тесты.
> Итоговое количество баллов определяется преподавателем после финального ревью.

## Устройство исправленной реализации

Основной код находится в трёх файлах. Публичные классы и функции из условия сохранены.

- `RateLimiter.kt`: проверка и регистрация попытки клиента под `Mutex`, затем ожидание общего `Semaphore`. Место освобождается в `finally`. Отмена принятой попытки не удаляет её из временного окна.
- `VoiceAssistant.kt`: основной путь STT → загрузка фактов → LLM → TTS. Фоновое извлечение фактов запускается после STT; независимые сохранения выполняются через `async`. В `callService` собраны выбор диспетчера, тайм-аут и повторы. Обычная отмена не превращается в ошибку сервиса.
- `StreamingJsonParser.kt`: `JsonFragmentReader` получает фрагменты и следит за временем и размером, `JsonDocumentParser` разбирает грамматику. Для каждого вида значения есть отдельный метод. События вложенных узлов выдаются сразу после их завершения, раньше события родительского узла.

### Границы потокового документа

Фрагменты склеиваются без добавления разделителей. Поэтому корневое число нельзя считать законченным только из-за конца фрагмента: после `1` может прийти `23`, и получится `123`. Для завершения числа нужен конец upstream или символ-разделитель, например пробел.

После завершения корня проверяются остаток текущего фрагмента и готовое продолжение upstream. Затем выдаётся `DocumentComplete`, и чтение прекращается. Ожидать произвольно поздний второй документ и одновременно немедленно завершаться на бесконечном источнике невозможно; парсер не ждёт будущих фрагментов после готового документа.

### Проверка

Для сборки нужен JDK 21. Полный запуск исходных и добавленных регрессионных тестов:

```bash
./gradlew --no-daemon test
```

В ограниченной среде можно отключить отдельный процесс компилятора и наблюдение за файлами:

```bash
./gradlew --no-daemon --no-watch-fs -Pkotlin.compiler.execution.strategy=in-process test
```

Исходные тесты задания сохранены. Дополнительные файлы `*RegressionTest.kt` проверяют отмену, диспетчер, дробные временные окна, разбиение JSON в разных местах, строгую грамматику, выдачу узлов до завершения документа и глубокую вложенность.
