# Спека: события доступа через Kafka (Gateway → SPPR)

> Статус: **согласованная спека** для переписывания Kafka-части PR #1 (Gateway)
> и PR #2 (SPPR Ingestor). Раскладка кода — по `messenger-overview`
> (UserServer → `user-created`/`user-deleted` → ChatServer). Семантика доставки —
> по требованиям ВКР (шаги 2.5, 3.1, 4.4 плана).

## 1. Требования

| Источник | Требование |
|---|---|
| 2.5 | Выключенная Kafka не меняет задержку GET/PUT; запрос не блокируется и не падает |
| 2.5, 3.1 | Потеря события обнаруживается по номерам: пропуск записан и виден |
| 3.1 | Повтор события не увеличивает спрос |
| 2.5 | Миграции отличаются от клиентских обращений |
| 3.2, 3.3 | Есть история записей: возраст версии, новые объекты |
| 4.4 | Рост p95 GET/PUT с событиями ≤ 5 %; события можно выключить для режима «без расширения» |

Не требуется: гарантированная доставка каждого события (outbox). Потеря допустима,
незамеченная потеря — нет.

## 2. Принципы

1. **Инфраструктура — только `shared/pkg/go-kit/kafka`**: `kafka.Producer`,
   `kafka.Consumer`, `consumer.Middleware`, `kafka.Message`. Сервисы не пишут
   своих очередей, воркеров и обёрток над `sarama.ConsumerGroup`.
2. **Один топик — один тип события — одно proto-сообщение.**
3. **Контракт — в `shared/api/events/v1/`**, генерация `make generate`.
4. **Kafka-код в фиксированных местах** (раздел 5); доменные сервисы видят только
   `AccessProducerService` (Gateway) и `Ingestor` (SPPR).
5. **Async publish без своей очереди**: буфер, ретраи и порядок даёт
   `sarama.AsyncProducer`; `Send` не ждёт Kafka (передача в sarama — около 1 мс,
   см. 4.1).
6. **Обнаружение потерь — через поток с номерами** (`stream_id` + `sequence`),
   дедупликация — inbox по `(gateway_id, stream_id, sequence)`.

## 3. Топики и контракт

| Топик | Proto | Key | Producer | Consumer |
|---|---|---|---|---|
| `object-read` | `events.v1.ObjectRead` | `{gateway_id}/{stream_id}` | Gateway | SPPR |
| `object-written` | `events.v1.ObjectWritten` | `{gateway_id}/{stream_id}` | Gateway | SPPR |

```proto
// shared/api/events/v1/access.proto
syntax = "proto3";
package events.v1;
import "google/protobuf/timestamp.proto";
option go_package = "github.com/alesplll/opens3-rebac/shared/pkg/go/events/v1;events_v1";

enum Origin {
  ORIGIN_UNSPECIFIED = 0;
  ORIGIN_CLIENT = 1;
  ORIGIN_MIGRATION = 2;
}

// Позиция события в потоке одного процесса Gateway.
message StreamPosition {
  string gateway_id = 1;
  string stream_id = 2;  // новый UUID на каждый старт Gateway и на каждый топик
  int64 sequence = 3;    // с 1, без дыр на стороне producer
}

message ObjectRead {
  StreamPosition position = 1;
  google.protobuf.Timestamp occurred_at = 2;
  string bucket = 3;
  string object_key = 4;
  string version_id = 5;
  int64 bytes = 6;       // длина отданного диапазона (Range) или размер объекта
  Origin origin = 7;
}

message ObjectWritten {
  StreamPosition position = 1;
  google.protobuf.Timestamp occurred_at = 2;
  string bucket = 3;
  string object_key = 4;
  string version_id = 5;
  int64 bytes = 6;
  Origin origin = 7;
}
```

Решения по контракту:

- **Поток = (gateway_id, stream_id) на один топик.** У каждого топика свой
  `stream_id` и свой счётчик `sequence`. Порядок между двумя топиками Kafka не
  гарантирует, поэтому общий счётчик дал бы ложные пропуски. Схема
  `ingestion_streams` при этом не меняется.
- **Key = `{gateway_id}/{stream_id}`** — весь поток в одном partition, порядок
  сохраняется. Pinned partitioner не нужен: число partition'ов не меняем.
- **Публикуются только успешные операции.** Ошибки — метрики и логи Gateway.
  Поля `operation` и `result` уходят: операцию задаёт топик.
- **Миграции** — `origin = ORIGIN_MIGRATION`. Как Gateway узнаёт, что запрос от
  Migration Executor, решается в шаге 3.6; до этого всегда `ORIGIN_CLIENT`.
- `shared/pkg/go-kit/events/access` удаляется; проверка входящих событий
  переезжает в `services/sppr/internal/validator`.

## 4. Изменения в `shared/pkg/go-kit`

### 4.1 `kafka/producer/async.go` — неблокирующий producer

```go
var ErrBufferFull = errors.New("kafka producer buffer is full")

// NewAsyncProducer реализует kafka.Producer поверх общего sarama.AsyncProducer.
func NewAsyncProducer(ap sarama.AsyncProducer, topic string) kafka.Producer

// Send передаёт сообщение в ap.Input(): сначала select/default, затем ждёт
// около 1 мс (таймер). nil — принято sarama, ErrBufferFull — буферы полны,
// событие потеряно.
func (p *asyncProducer) Send(ctx context.Context, key, value []byte, _ kafka.PrettyDecoder) error

// LogErrors вычитывает ap.Errors() до закрытия producer'а: логирует и вызывает
// onError (метрика). Запускается один раз на sarama.AsyncProducer.
func LogErrors(ap sarama.AsyncProducer, logger Logger, onError func(topic string))
```

- `ap.Input()` в sarama небуферизованный: dispatcher забирает сообщения по
  одному и перекладывает в буферы `ChannelBufferSize`. Голый select/default
  отказывает, пока dispatcher занят предыдущим сообщением: при здоровой Kafka
  замер дал 3.5 % ложных `ErrBufferFull` на ~2000 msg/s и 21.5 % на 16
  параллельных отправителях. С ожиданием до 1 мс ложных потерь нет. Цена: при
  долгом отказе Kafka, когда буферы заполнены, каждый GET/PUT ждёт около 1 мс.
- Один `sarama.AsyncProducer` на процесс, по одному `kafka.Producer` на топик —
  как `SyncProducer` в эталоне.
- `Return.Successes = true`: `LogResults` читает успехи и ошибки; подтверждения нужны только при shutdown, HTTP их не ждёт.
- Окончательная ошибка доставки (ретраи исчерпаны) приходит в `Errors()`: событие
  потеряно, а SPPR увидит пропуск по номерам.
- Graceful shutdown: HTTP drain → остановка периодических heartbeat → подтверждённые обычные heartbeat обоих потоков (барьеры дотправки буфера) → подтверждённые closing heartbeat → `ap.Close()` через closer. При ошибке/таймауте закрытие не объявляется успешным.

### 4.2 `middleware/kafka/` — middleware consumer'а

- `logging.go` — `Logging(logger)` из эталона.
- `retry.go` — `Retry(interval)`: повторяет handler до успеха или отмены ctx.
  Нужен, потому что `group_handler.go` при ошибке делает `continue` без
  `MarkMessage`, и следующий успешный offset закоммитит позицию за упавшим
  сообщением. Заменяет `worker/ingestion` из PR #2.

`go-kit/kafka/{kafka.go,message.go,consumer/*}` не меняются.

## 5. Раскладка кода

### 5.1 Producer — Gateway (по образцу UserServer)

```
services/gateway/internal/
  config/
    interfaces.go                      # + KafkaConfig, SaramaConfig, AccessEventsConfig, ObjectReadProducerConfig, ObjectWrittenProducerConfig
    env/kafka.go                       # KAFKA_BOOTSTRAP → Brokers()
    env/sarama.go                      # *sarama.Config для AsyncProducer (раздел 6)
    env/access_events.go               # ACCESS_EVENTS_ENABLED, GATEWAY_ID
    env/object_read_producer.go        # KAFKA_OBJECT_READ_TOPIC → Topic()
    env/object_written_producer.go     # KAFKA_OBJECT_WRITTEN_TOPIC → Topic()
  model/access.go                      # ObjectReadEvent, ObjectWrittenEvent, Origin
  service/access_producer.go           # interface AccessProducerService
  service/producer/access/
    service.go                         # struct: gatewayID, два потока, два kafka.Producer; NewService
    stream.go                          # stream{id, mu, sequence}: next() + Send под одним мьютексом
    object_read.go                     # ProduceObjectRead: model → proto → stream.publish
    object_written.go                  # ProduceObjectWritten
    noop.go                            # NewNoopService() — для ACCESS_EVENTS_ENABLED=false
  service/object/get.go, put.go        # после успеха: s.access.ProduceObjectRead/Written
  app/service_provider.go              # AsyncProducer(), ObjectReadProducer(), ObjectWrittenProducer(), AccessProducerService()
```

```go
// service/access_producer.go
type AccessProducerService interface {
    ProduceObjectRead(ctx context.Context, event model.ObjectReadEvent)
    ProduceObjectWritten(ctx context.Context, event model.ObjectWrittenEvent)
}
```

Правила:

- **Методы не возвращают ошибку.** Событие — телеметрия; ошибка публикации
  логируется и считается метрикой внутри producer-сервиса. S3-операция её не видит.
- **Номер и отправка — под одним мьютексом потока**: `sequence++` → marshal →
  `Send`. Иначе событие N+1 может попасть в `Input()` раньше N, и SPPR получит
  нарушение порядка. `Send` не блокирует, поэтому мьютекс держится микросекунды.
- **`ErrBufferFull` номер не возвращает**: номер потрачен, SPPR увидит пропуск.
- **Вызов — из service-слоя.** `object.Put` — после `CreateVersion`.
  `object.Get` — после открытия потока из Storage, `bytes` = длина диапазона.
  HTTP-хендлеры про события не знают; `countReader`/`countWriter` не нужны.
  Чтение, оборванное на середине стрима, считается чтением: спрос был.
- **Выключение** (`ACCESS_EVENTS_ENABLED=false`): provider отдаёт `noop`-сервис,
  Kafka-клиент не создаётся. Никаких `if h.access != nil`.
- Время `occurred_at` — `time.Now().UTC()` в момент присвоения номера, поэтому
  внутри потока оно монотонно вместе с `sequence`.

### 5.2 Consumer — SPPR (по образцу ChatServer)

```
services/sppr/internal/
  config/
    interfaces.go                      # KafkaConfig, SaramaConfig(GroupID), ObjectReadConsumerConfig, ObjectWrittenConsumerConfig, RetryConfig
    env/kafka.go                       # KAFKA_BOOTSTRAP
    env/sarama.go                      # KAFKA_CONSUMER_GROUP_ID, OffsetOldest
    env/object_read_consumer.go        # KAFKA_OBJECT_READ_TOPIC
    env/object_written_consumer.go     # KAFKA_OBJECT_WRITTEN_TOPIC
  converter/kafka/kafka.go             # ObjectReadDecoder, ObjectWrittenDecoder
  converter/kafka/decoder/
    object_read_decoder.go             # proto.Unmarshal → model.AccessEvent{Operation: read}
    object_written_decoder.go          # → model.AccessEvent{Operation: write}
  service/consumer/access/
    consumer.go                        # consumer+decoder на топик, ingestor; RunConsumer: errgroup по топикам
    handlers.go                        # ObjectReadHandler/ObjectWrittenHandler: decode → Ingest → классификация ошибки
  service/ingestor/                    # без изменений по сути: validate + tx + repo.Store
  repository/event/                    # логика потоков, пропусков, дублей и агрегатов — как сейчас
  validator/event.go                   # проверка из go-kit/events/access переезжает сюда
  app/service_provider.go              # ConsumerGroup на топик, Consumer, Decoder, AccessConsumerService
  app/app.go                           # go RunConsumer(ctx) рядом с health HTTP; закрытие через closer
```

Правила:

- `handlers.go` получает `kafka.Message`, декодирует, вызывает
  `Ingestor.Ingest(ctx, event, model.Delivery{Topic, Partition, Offset})`.
- **Классификация ошибок — ключевое правило consumer'а:**

  | Ошибка | Пример | Действие |
  |---|---|---|
  | Инфраструктурная | БД недоступна, таймаут, serialization failure | вернуть ошибку → `Retry` повторяет, partition ждёт |
  | Отклонённое событие | не декодируется, не проходит валидацию, позднее событие из пропуска, тот же номер с другим payload, время назад | лог + метрика, вернуть `nil` → offset коммитится |

  Ingestor помечает отклонения ошибкой `model.ErrRejected` (`errors.Is`).
  Пропущенное отклонённое событие само становится пропуском по номерам — потеря
  видна в `ingestion_gaps`, а partition не блокируется навсегда.

- Дедупликация (inbox) — как в текущем `store.go`: `PRIMARY KEY (gateway_id,
  stream_id, sequence)` + сверка `payload_hash`; событие, прогресс потока,
  пропуск и минутный агрегат — в одной транзакции `ReadCommitted`.
- `payload_hash` считается от исходных байтов `msg.Value`, а не от повторного
  `json.Marshal`.
- Readiness по числу активных сессий consumer group уходит; health проверяет PG.

## 6. Конфигурация

| Env | Gateway | SPPR | В `.env` |
|---|---|---|---|
| `ACCESS_EVENTS_ENABLED` | ✓ | — | `true` |
| `GATEWAY_ID` | ✓ (обязателен при enabled) | — | `gateway-1` |
| `KAFKA_BOOTSTRAP` | ✓ | ✓ | `kafka:29092` |
| `KAFKA_OBJECT_READ_TOPIC` | ✓ | ✓ | `object-read` |
| `KAFKA_OBJECT_WRITTEN_TOPIC` | ✓ | ✓ | `object-written` |
| `KAFKA_CONSUMER_GROUP_ID` | — | ✓ | `sppr` |
| `KAFKA_RETRY_INTERVAL` | — | ✓ | `1s` |
| `ACCESS_HEARTBEAT_INTERVAL` | ✓ | — | `5s` |
| `INGEST_STREAM_LOST_AFTER` | — | ✓ | `30s` |
| `PLANNER_MAX_HISTORY_LAG` | — | ✓ | `60s` |

Параметры sarama — константы в `config/env/sarama.go`, не env:

| Gateway (`AsyncProducer`) | Значение | Зачем |
|---|---|---|
| `Producer.Idempotent` | `true` | ретраи без дублей и перестановок |
| `Producer.RequiredAcks` | `WaitForAll` | требование idempotent |
| `Net.MaxOpenRequests` | `1` | требование idempotent, порядок |
| `Producer.Return.Successes` / `Errors` | `true` / `true` | оба канала читает `LogResults`, shutdown ждёт барьеры |
| `ChannelBufferSize` | `1024` | объём буфера до первой потери |
| `Producer.Retry.Max`, `Retry.Backoff` | `5`, `200ms` | переживаем короткую недоступность Kafka |
| `Version` | `V2_8_0_0` | idempotent producer |

| SPPR (`ConsumerGroup`) | Значение |
|---|---|
| `Consumer.Offsets.Initial` | `OffsetOldest` |
| `Consumer.Return.Errors` | `true` (вычитываются и логируются) |

## 7. Отказоустойчивость

| Отказ | Поведение | Чем обеспечено |
|---|---|---|
| Kafka недоступна недолго | GET/PUT не замедляются, события дотправляются | буфер и ретраи `AsyncProducer` |
| Kafka недоступна долго | GET/PUT ждут передачу в sarama около 1 мс, часть событий теряется, пропуск виден в SPPR | `ErrBufferFull` / `Errors()` + `sequence` |
| Рестарт / падение Gateway | новый `stream_id`; буфер при graceful stop дотправляется, при падении теряется | `closer` + `ap.Close()`; хвост — см. ниже |
| SPPR или его PG недоступны | события ждут в Kafka, ingestor догоняет | commit offset после транзакции + `Retry` |
| Дубли (ретрай producer, redelivery) | спрос не растёт | inbox по `(stream, sequence)` + `payload_hash` |
| Отклонённое событие | partition не блокируется, потеря видна как пропуск | классификация ошибок (5.2) |

### Полнота истории и хвост потока (этап 5)

`events.v1.StreamHeartbeat{position, at, closing}` идёт в тот же топик с тем же
ключом `{gateway_id}/{stream_id}` и заголовком `event-type: heartbeat`.
`position.sequence` — последний выданный номер, включая потери; heartbeat номер
не тратит. События без заголовка сохраняют прежний контракт. Неизвестный
`event-type` и некорректный heartbeat отклоняются с commit offset; ошибка БД
удерживает partition через Retry. Нумерация, timestamp, marshal и Send событий и
heartbeat сериализованы одним mutex потока.

После commit heartbeat Ingestor атомарно сохраняет прогресс, receipt с hash
исходных байтов и хвостовой gap `last_sequence+1 … heartbeat.sequence` при наличии
потери. Его `after_time` — последнее событие, `before_time` — время heartbeat.
Повтор тех же байтов (включая новый Kafka offset) не меняет прогресс и gaps.
Повтор Kafka offset с другим содержимым, обратное время/номер и продолжение graceful-closed
потока отклоняются; точные повторы старых событий продолжают дедуплицироваться.

В `ingestion_streams` хранятся `complete_until`, первый/последний heartbeat,
`closed_at`, `lost_at` и время регистрации. Для потока без потерянных номеров
`complete_until = heartbeat.at`; если потеряны номера — время последнего события.
Если ни одного события не доставлено при положительном номере, граница остаётся
неизвестной. Простое продвижение heartbeat не объявляет потерянные номера
полученными. При graceful stop closing закрывает поток без дополнительного gap.

Открытый поток без heartbeat **дольше** `INGEST_STREAM_LOST_AFTER` объявляется
потерянным фоновой проверкой (период timeout/6). Потоки без единого heartbeat
отсчитываются от времени регистрации; трафик событий этот срок не продлевает.
В одной транзакции поток закрывается и добавляется gap
`[complete_until, lost_at)` с `last_sequence = NULL` (число потерь неизвестно).
Если граница ещё неизвестна, нижняя граница берётся консервативно от первого
события/регистрации. Закрытие по lost-check не окончательно: новое допустимое
сообщение того же потока снимает `closed_at`/`lost_at` атомарно со своей записью.
При непрерывном номере (событие `last_sequence+1`, heartbeat `last_sequence`)
удаляется только помеченный `is_lost` gap текущего lost-check с `before_time=lost_at`.
При скачке номера он сохраняется вместе с новым gap известных номеров.
Graceful closing остаётся окончательным; повторы не открывают поток снова.
Счётчик/время возобновления и recovery runbook — в [SPPR README](../services/sppr/README.md#восстановление-потерянного-потока).

`repository/history.Coverage` вычисляет W как минимум `complete_until` открытых
потоков. Открытый поток с неизвестной границей делает W неизвестной. Если открытых
нет, берётся максимум границ закрытых, ограниченный моментом последнего закрытия.
Без потоков полнота неизвестна. Закрытые с неизвестной границей не дают значения
W, но их gaps остаются видны; потерянные входят также в число закрытых.
`last_event_at` одного потока не является общей W.

Planner считает признаки на W. Чтения ограничены `occurred_at < W` **до**
группировки по минутам: текущий минутный агрегат мог уже получить будущие чтения.
Начало истории по-прежнему расширяется до самой старой версии каталога, но W не
подменяется текущим временем. При неизвестной W или `now-W >
PLANNER_MAX_HISTORY_LAG` сохраняется `history_incomplete` без рекомендаций;
причина отставания содержит величину lag. Gaps делают пересекающие их окна
непригодными, включая gaps с неизвестным числом событий.

`/api/planner/status` содержит `history` (W, known, streams, gaps_24h),
`history_lag_seconds` и `objects` с качеством и состояниями «чтений нет» /
«история неполна». `/admin` показывает эти же значения и карточки объектов,
включая объекты без готовой рекомендации. Метки обучения используют ту же
Coverage: пригодны только при W ≥ t+H и отсутствии gaps в окне метки.

Проверка: `make smoke-history-local` запускает отдельный стенд, останавливает
Kafka, делает kill/stop Gateway и проверяет шесть сценариев
[спецификации этапа 5](history-completeness-spec.md). Подробности и ограничения
запуска — в [SPPR README](../services/sppr/README.md#полнота-истории-и-smoke-history).

## 8. Что уходит из PR

**PR #1 (Gateway):** `internal/publisher/access` (своя очередь, retry-цикл, drain,
`pending`/`dropped`), `internal/app/access.go` (ручной sarama client, pinned
partitioner), `config/env/access.go` (8 env и валидация), `handler/httpapi/count.go`
и изменения хендлеров, `go-kit/kafka/producer/async.go` в текущем виде (заменяется
на 4.1), `go-kit/events/access`.

**PR #2 (SPPR):** `worker/ingestion` (observedGroup, sessionHandler, свой цикл
перезапуска), `handler/access` (ручной JSON-декодер), `KAFKA_PROCESS_TIMEOUT`,
`KAFKA_MAX_EVENT_BYTES`, readiness по сессиям.

**Остаются:** схема БД (`ingestion_streams`, `access_events`, `ingestion_gaps`,
`read_aggregates`), `repository/event/store.go`, `service/ingestor`, транзакции,
интеграционные тесты репозитория.

## 9. Тесты

- **go-kit:** `async_test.go` — `Send` при полном `Input()` возвращает
  `ErrBufferFull` без блокировки (sarama `mocks.AsyncProducer`); `retry_test.go`.
- **Gateway:** `service/producer/access` — номера идут подряд, `ErrBufferFull`
  тратит номер, отдельный счётчик на топик; `service/object` — вызов
  `AccessProducerService` только после успеха (minimock).
- **SPPR:** handlers — классификация ошибок; decoders; существующие
  интеграционные тесты `store.go`.
- **Smoke:** PUT/GET → Kafka → SPPR; выключенная Kafka не меняет задержку GET;
  после включения пропуск записан в `ingestion_gaps`.
