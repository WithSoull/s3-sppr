# SPPR: маршрут чтения и первая проверка

Этот текст объясняет текущую реализацию. Команды запуска, полный контракт и
переменные окружения собраны в [README](README.md). Решения по Kafka — в
[спеке](../../docs/kafka-spec.md).

## Что сервис делает сейчас

SPPR реализует Ingestor: получает события обращений из Kafka, сохраняет
историю в PostgreSQL и считает минутный спрос на версии объектов. Второй
компонент — исполнитель переносов HOT/COLD: принимает действия через HTTP API и
переносит версии между Storage (маршрут чтения — [в конце](#исполнитель-переносов)).
Предсказаний и автоматического выбора HOT/COLD здесь ещё нет.

Gateway уже существует: он обслуживает HTTP PUT/GET через Metadata и Storage.
Producer обращений вынесен в независимый [PR #1](https://github.com/WithSoull/s3-sppr/pull/1).
При `ACCESS_EVENTS_ENABLED=true` он публикует успешные GET в `object-read`, а
PUT — в `object-written`. До его мержа SPPR проверяется integration tests.
События Storage `object-stored` не заменяют историю чтений.

```mermaid
flowchart LR
    Client[HTTP клиент] --> GW[Gateway]
    GW --> Metadata
    GW --> Storage
    GW -. Producer из отдельного PR .-> AP[sarama AsyncProducer]
    AP --> Read[Kafka object-read]
    AP --> Written[Kafka object-written]
    Read --> Consumer[go-kit Consumer + Retry]
    Written --> Consumer
    Consumer --> Handlers[Handlers + decoders]
    Handlers --> Ingestor
    Ingestor --> Repo[Event repository]
    Repo --> PG[(PostgreSQL)]
```

## Сначала разберись с тремя идентификаторами

| Идентификатор | Что обозначает | Где используется |
|---|---|---|
| `gateway_id / stream_id / sequence` | конкретное событие производителя | дедупликация и поиск пропусков |
| `topic / partition / offset` | запись в Kafka | координаты доставки и подтверждение consumer group |
| `bucket / object_key / version_id` | версию объекта, к которой обращались | история и агрегаты спроса |

`gateway_id` остаётся стабильным для экземпляра Gateway. `stream_id` меняется
при каждом новом запуске producer, и у каждого topic он свой. `sequence`
начинается с 1 и растёт внутри этого потока. Разные потоки не обязаны иметь
общее время или общую нумерацию.

Kafka key — строка `gateway_id/stream_id`. Все события потока должны приходить
в одну partition в порядке номеров. Offset назначает Kafka, sequence — producer:
это разные счётчики. Между двумя событиями одного Gateway в partition могут
находиться сообщения других Gateway.

Если потерян номер 2, а затем пришёл номер 3, SPPR запишет пропуск. Если потерян
последний номер и больше ничего не пришло, обнаружить потерю пока невозможно.
Для этого нужны heartbeat и сведения о завершении потока.

## Порядок чтения кода

Иди в этом порядке: сначала обработка одного события, затем доставка и запуск.
Все ссылки ниже ведут на исходники внутри репозитория.

### 1. Модель и контракт

Прочитай контракт [access.proto](../../shared/api/events/v1/access.proto), затем
[model/event.go](internal/model/event.go) и
[validator/event.go](internal/validator/event.go).

`AccessEvent` — содержимое события и исходные байты Kafka (`Payload`) для
сравнения повторов. `Delivery` — координаты Kafka, полученные consumer, а не
взятые из сообщения. `IngestResult` сообщает, был ли повтор и сколько номеров
пропущено перед новым событием. `ErrRejected` помечает событие, которое повтор
обработки не исправит.

`IsDemand()` выделяет только `read + client + success`. Запись и чтение при
миграции остаются в истории, но не повышают спрос. Иначе система начала бы
считать собственные переносы пользовательским интересом. Gateway публикует только
успешные операции, поэтому `result` всегда `success`.

Validator проверяет идентификаторы, допустимые операции и origin, время и длины
строк. `version_id` обязателен. Количество байтов может быть нулевым: пустой
объект тоже можно успешно прочитать.

### 2. Схема PostgreSQL

Прочитай [up-миграцию](migrations/20261021000000_ingestion.up.sql).

Четыре таблицы хранят прогресс потока, историю обращений, пропуски и минутный спрос:

| Таблица | Одна строка — это | Для чего нужна |
|---|---|---|
| `ingestion_streams` | один запуск producer одного Gateway | хранить прогресс и блокировать поток при конкурентной обработке |
| `access_events` | одно принятое событие обращения | хранить точную историю и узнавать повторы |
| `ingestion_gaps` | один диапазон пропущенных номеров в потоке | явно отличать потерянные события от отсутствия спроса |
| `read_aggregates` | спрос на одну версию объекта за одну минуту | читать готовые счётчики без пересчёта всей истории |

На схеме показаны ключи и основные поля; служебные времена `received_at`,
`detected_at`, `updated_at` и поле `first_event_at` смотри в миграции.
Сплошные линии — реальные внешние ключи PostgreSQL.

```mermaid
erDiagram
    ingestion_streams ||--o{ access_events : "события этого потока"
    ingestion_streams ||--o{ ingestion_gaps : "пропуски этого потока"

    ingestion_streams {
        text gateway_id PK
        text stream_id PK
        text topic
        integer partition
        bigint last_sequence "последний принятый номер"
        timestamptz last_event_at "время последнего события"
        bigint missing_events "сколько номеров пропущено"
    }

    access_events {
        text gateway_id PK,FK
        text stream_id PK,FK
        bigint sequence PK
        timestamptz occurred_at
        text bucket
        text object_key
        text version_id
        text operation "read или write"
        text origin "client или migration"
        text result "success или error"
        bigint bytes
        bytea payload_hash "сравнение содержимого при повторе"
        text topic UK
        integer partition UK
        bigint kafka_offset UK
    }

    ingestion_gaps {
        text gateway_id PK,FK
        text stream_id PK,FK
        bigint first_sequence PK "начало пропуска включительно"
        bigint last_sequence "конец пропуска включительно"
        timestamptz after_time "NULL если предыдущего события нет"
        timestamptz before_time "время события после пропуска"
    }

    read_aggregates {
        text bucket PK
        text object_key PK
        text version_id PK
        timestamptz minute PK "начало минуты UTC"
        bigint reads "успешные клиентские чтения"
        bigint bytes "сумма байтов этих чтений"
    }
```

Как читать ключи и связи:

- Все поля с `PK` **внутри одной таблицы вместе** образуют её первичный ключ.
  Например, событие узнаётся по тройке `(gateway_id, stream_id, sequence)`;
  один `sequence` не уникален для всей базы.
- `FK` у пары `(gateway_id, stream_id)` ведёт в `ingestion_streams`. Событие и
  пропуск всегда принадлежат существующему потоку. У потока может быть много
  событий и много диапазонов пропусков.
- Три поля с `UK` в `access_events` образуют **одно общее ограничение UNIQUE**:
  `(topic, partition, kafka_offset)`. Оно защищает от привязки одной записи Kafka
  к разным событиям. Отдельный offset между разными partitions может совпадать.

У `read_aggregates` **нет внешнего ключа к `access_events`**. Это отдельный
результат подсчёта: repository увеличивает его только для
`operation=read AND origin=client AND result=success`. Группировка идёт по
`(bucket, object_key, version_id, minute)`, поэтому события нескольких потоков
могут увеличивать одну строку агрегата. Это логическая зависимость данных,
а не ещё одна связь `FK` на схеме.

`bucket`, `object_key`, `version_id` адресуют объект из Metadata. В базе SPPR
нет локальных таблиц buckets/versions и внешних ключей в БД другого сервиса.

**Пример после обработки `1 → повтор 1 → 3`.** Все события относятся к потоку
`gw-1/run-1`, версии `demo/object.bin/v1`; события 1 и 3 — успешные клиентские
чтения по 100 байт в `12:00:01` и `12:00:03` UTC одного дня.

| Таблица | Что окажется в базе |
|---|---|
| `ingestion_streams` | одна строка `gw-1/run-1`: `last_sequence=3`, `last_event_at=12:00:03`, `missing_events=1` |
| `access_events` | две строки: `sequence=1` и `sequence=3`; идентичный повтор 1 не создаёт строку |
| `ingestion_gaps` | одна строка: `first_sequence=2`, `last_sequence=2`, `after_time=12:00:01`, `before_time=12:00:03` |
| `read_aggregates` | одна строка для `demo/object.bin/v1`, `minute=12:00:00`: `reads=2`, `bytes=200` |

Событие, новый gap, счётчики и прогресс меняются **в одной транзакции**.
Например, если обновление агрегата для события 3 завершится ошибкой, в базе
останется состояние после события 1: без события 3, без нового gap и с
`last_sequence=1`. Повторная доставка сможет выполнить всю операцию заново.

`after_time` у первого пропуска может быть `NULL`: если первым пришёл номер 5,
время событий 1–4 неизвестно. `before_time` — время первого события после
пропуска. Это границы неопределённости, а не восстановленные времена операций.

В минутном агрегате нет `gateway_id`: спрос разных Gateway на одну версию
складывается. Точные события сохраняются отдельно, поскольку минутный агрегат
не позволяет точно обрезать выборку посередине минуты.

`down.sql` удаляет эти четыре таблицы; это не команда обычного перезапуска.
Миграции применяет отдельный `migrate` контейнер, приложение их не запускает.

### 3. Сервис и транзакция

Прочитай [service/ingestor/service.go](internal/service/ingestor/service.go).

Здесь проверяются доменное событие и `Delivery`; нарушение возвращается как
`ErrRejected`. Затем время приводится к UTC с точностью до микросекунды. Это
важно для сравнения с PostgreSQL: разные представления одного времени не должны
давать ложную регрессию.

`ReadCommitted` открывает транзакцию, вызывает repository и возвращается после
commit. При ошибке commit сервис возвращает ошибку и пустой результат: нельзя
объявлять событие сохранённым по результату ещё не зафиксированной операции.

После этого загляни в
[shared transaction manager](../../shared/pkg/go-kit/client/db/transaction/transaction.go).
Он помещает `pgx.Tx` в context. Методы shared PG client достают его оттуда и
выполняют запросы через транзакцию. Repository не хранит отдельный `pgx.Tx`.

### 4. Основной алгоритм сохранения

Внимательно прочитай [repository/event/store.go](internal/repository/event/store.go).
Это основная логика сервиса.

1. Убедиться, что вызов выполняется внутри транзакции.
2. Создать строку потока, если её ещё нет, и взять `FOR UPDATE` на эту строку.
3. Проверить, что поток не переехал в другой topic или partition.
4. Посчитать SHA-256 от исходного значения Kafka (`Payload`).
5. Если sequence уже обработан, сравнить hash с сохранённым событием.
6. Для нового sequence проверить порядок времени, вставить событие и пропуск.
7. При `IsDemand()` увеличить минутный агрегат.
8. Обновить прогресс потока. Commit выполнит внешний transaction manager.

Блокировка нужна, чтобы два обработчика одного потока не прочитали одинаковый
старый прогресс и не посчитали спрос дважды. Для разных потоков общий агрегат
обновляется через атомарный `INSERT ... ON CONFLICT DO UPDATE`.

Пример: пришли `1, 1, 3`. Первый `1` записывается; второй с тем же содержимым
возвращает `Duplicate`; `3` создаёт gap `2..2`. Если это успешные клиентские
чтения по 100 байт, получится два чтения и 200 байт.

Повтор `1` с другим количеством байтов — конфликт идентичности. Поздний `2`
после сохранённого `3` также отклоняется: автоматического заполнения gap здесь
нет. Это выбранное ограничение восстановления истории. Такие нарушения store
возвращает как `ErrRejected`; ошибки SQL и commit — как обычные ошибки.

Hash считается по байтам сообщения, которые Gateway сериализовал один раз:
ретрай producer и повторная доставка приносят те же байты. Координаты Kafka в
hash не входят: одинаковое событие может быть повторно опубликовано с другим offset.

Если сломалось обновление агрегата, откатываются также событие, gap и прогресс.
Иначе повторная доставка могла бы увидеть уже сохранённый event и потерять спрос.

### 5. Decoders и handlers

Прочитай [converter/kafka/kafka.go](internal/converter/kafka/kafka.go), decoders в
[converter/kafka/decoder](internal/converter/kafka/decoder) и
[handlers.go](internal/service/consumer/access/handlers.go).

Decoder разбирает protobuf своего topic и задаёт операцию: `ObjectRead` → `read`,
`ObjectWritten` → `write`. Отсутствующие поля остаются пустыми, их отклонит
validator. Handler вызывает decoder, затем Ingestor с реальными координатами
доставки и классифицирует ошибку:

- не декодируется или `errors.Is(err, model.ErrRejected)` — лог, метрика
  `rejected`, возврат `nil`: offset подтверждается, номер станет пропуском;
- любая другая ошибка — лог, метрика `error`, возврат ошибки в `Retry`.

Это транспортная граница: SQL здесь отсутствует. Валидация остаётся в сервисном
слое, чтобы её нельзя было обойти прямым вызовом Ingestor.

### 6. Consumer, Retry и подтверждение Kafka

Прочитай [consumer.go](internal/service/consumer/access/consumer.go), затем
[shared consumer](../../shared/pkg/go-kit/kafka/consumer/concumer.go),
[group handler](../../shared/pkg/go-kit/kafka/consumer/group_handler.go) и
[Retry](../../shared/pkg/go-kit/middleware/kafka/retry.go).
Имя `concumer.go` — существующее имя файла в общей библиотеке.

`RunConsumer` запускает по consumer на topic в одном errgroup: ошибка одного
останавливает оба, и приложение завершается.

Shared consumer после ошибки handler продолжает читать сообщения. Просто вернуть
ошибку из SPPR недостаточно: более позднее успешное сообщение позволило бы
подтвердить offset поверх необработанного события.

Поэтому `Retry` оборачивает handler. После ошибки он ждёт `KAFKA_RETRY_INTERVAL`
и повторяет то же сообщение до успеха либо отмены сессии. После отмены он не
обрабатывает и следующие сообщения claim. Только после успеха consumer вызывает
`MarkMessage`.

`MarkMessage` отмечает offset локально; broker commit выполняет Sarama. Если
процесс упадёт после PostgreSQL commit и до Kafka commit, событие придёт снова,
а repository распознает повтор. Здесь доставка с повторами и однократное
изменение данных в БД благодаря дедупликации.

Инфраструктурная ошибка блокирует свою partition, а не все partitions; readiness
от этого не меняется. Отклонённое событие partition не блокирует. DLQ нет.
Отмена context завершает ожидание retry. Сетевые таймауты Sarama используют её
defaults.

### 7. Сборка зависимостей и запуск

Теперь прочитай [service_provider.go](internal/app/service_provider.go),
[app.go](internal/app/app.go) и [main.go](cmd/server/main.go).

Provider собирает PG client → repository и transaction manager → Ingestor; для
каждого topic — sarama ConsumerGroup → go-kit Consumer с `Logging` и `Retry` →
decoder; затем `AccessConsumerService`. Каждый созданный ресурс сразу
регистрируется в closer. Зависимости создаются лениво, но разрешаются на одной
goroutine до запуска обработки. Методы provider не рассчитаны на конкурентную
первоначальную сборку.

Перед consumer проверяется наличие таблиц. Это проверка наличия схемы, а не
проверка всех колонок или номеров миграций.

`NewApp` настраивает logger, closer, tracing, metrics, зависимости и HTTP
listener. `Run` запускает HTTP и `RunConsumer`. Сигнал из main отменяет общий
context; `Run` завершает HTTP и ждёт остановки consumers. После `Run`
вызывается `Close`: closer параллельно закрывает consumer groups, PG pool,
listener, metrics и traces, затем закрывается logger. Параллельное закрытие
безопасно, потому что обработка к этому моменту остановлена. Ошибки частичного
startup тоже приводят к очистке уже созданных ресурсов.

### 8. Конфигурация, health и telemetry

Прочитай [config.go](internal/config/config.go),
[interfaces.go](internal/config/interfaces.go) и файлы
[config/env](internal/config/env). В них только разбор env и проверка настроек:
`pg.go`, `kafka.go`, `sarama.go`, `object_read_consumer.go`,
`object_written_consumer.go`, `retry.go`, `http.go`, `logger.go`, `tracing.go`,
`metrics.go`.

Настройки окружения важнее `.env`; путь задаётся флагом `-config-path`.
`PG_DSN` важнее отдельных PG-полей. Образ использует `GOWORK=off` и локальный
`replace` на `shared`, поэтому ему не нужны исходники остальных сервисов.

Заверши чтением [health handler](internal/handler/health/handler.go) и
[ingestion metrics](internal/observability/metrics.go).

`/healthz` показывает, что HTTP server отвечает. `/readyz` требует успешного PG
ping. Он не отражает Kafka и не доказывает отсутствие gaps и готовность истории
к обучению модели.

Счётчики telemetry описывают попытки процесса. `read_aggregates` описывает спрос
в зафиксированных данных. Это разные величины: retry увеличивает число ошибок,
но не должен увеличивать чтения. В label метрик не попадают object key и stream
ID, чтобы число временных рядов не росло с каждым объектом.

## Какие тесты читать после реализации

| Файл | Какую гарантию показывает |
|---|---|
| [ingest_test.go](internal/service/ingestor/ingest_test.go) | валидация до БД (`ErrRejected`), нормализация времени, ошибки repository и commit |
| [handlers_test.go](internal/service/consumer/access/handlers_test.go) | классификация ошибок: подтверждение отклонённых, повтор инфраструктурных |
| [decoder_test.go](internal/converter/kafka/decoder/decoder_test.go) | protobuf → `AccessEvent`, origin, пустые поля и мусор |
| [event_test.go](internal/validator/event_test.go) | правила контракта |
| [retry_test.go](../../shared/pkg/go-kit/middleware/kafka/retry_test.go) | повтор того же сообщения и отмена ожидания |
| [store_integration_test.go](internal/repository/event/store_integration_test.go) | реальные SQL: дедупликация, gap, спрос, конкуренция, отклонения и полный rollback |
| [app_integration_test.go](internal/app/app_integration_test.go) | Kafka → PG по двум topics, ожидание partition, подтверждение мусора, offset, health, OTLP и shutdown |
| [close_test.go](internal/app/close_test.go) | освобождение HTTP-порта до первого Run |
| [pg_test.go](internal/config/env/pg_test.go) | сохранение спецсимволов и IPv6 при сборке подключения |

`pkg/mocks/*_minimock.go` — сгенерированная механика. Для понимания достаточно
двух интерфейсов в [service.go](internal/service/service.go),
[repository.go](internal/repository/repository.go) и команды генерации в
[generate.go](pkg/mocks/generate.go). Построчно читать generated mocks не нужно.

## Запусти и проверь руками

Из корня репозитория:

```bash
make test-sppr
make up-sppr
curl -f http://localhost:8086/readyz
```

Readiness зависит только от PostgreSQL. Присоединение к Kafka группе видно в
`docker compose logs sppr`. Если другой Compose-проект уже занял 9092 или 5436,
сначала проверь
`docker compose ls`: один стенд должен использовать согласованное имя проекта
(`docker compose -p <имя>`). Не запускай второй broker на том же host-порту.

События — бинарный protobuf, поэтому консольный producer Kafka для ручного
примера не подходит. Последовательность `1, 1, 3`, сбой БД и мусорное сообщение
воспроизводит готовый интеграционный тест. Он создаёт временные schema и topics
и удаляет их после себя:

```bash
SPPR_TEST_PG_DSN='postgres://sppr_user:sppr_password@localhost:5436/sppr_db?sslmode=disable' \
SPPR_TEST_KAFKA_BOOTSTRAP=localhost:9092 \
make test-sppr-integration
go vet ./services/sppr/...
```

Kafka-проверка пропускается без её env. PostgreSQL-проверки требуют явно заданный
DSN. В [CI](../../.github/workflows/sppr-ci.yml) обе зависимости поднимаются
отдельно; там проверяются vet и тесты с `-race`.

## Исполнитель переносов

Это отдельный путь, Kafka он не использует. Сначала прочитай протокол Metadata
([README Metadata](../metadata/README.md#перенос-версии-между-классами)): исполнитель
только вызывает его RPC и копирует байты.

1. `internal/model/migration.go` — статусы, шаги, причины и `Transition`.
2. `migrations/20261028000000_migration.up.sql` — действия, рёбра, журнал,
   списания бюджета (одна строка на попытку), настройки, закрепления.
3. `internal/service/migration/` и `internal/handler/migration/` — Submit с
   идемпотентностью по `action_id`, отмена, бюджет, `blocked_by`.
4. `internal/repository/action/write.go` — `Claim` (`FOR UPDATE SKIP LOCKED` под
   блокировкой строки настроек) и `Transition` (статус, списание, журнал и каскад
   `dependency_failed` в одной транзакции).
5. `internal/service/executor/process.go` — шаги действия. `process` начинается с
   `ListMigrationCopies`, поэтому новое и прерванное рестартом действие идут одним
   путём. `copy.go` — поток Storage → Storage с подсчётом MD5.
6. `internal/service/cleanup/cleanup.go` — отдельный цикл, который не читает флаг
   паузы: due-копии всех ролей → Storage Delete → CompleteCleanup. Ошибка Storage
   сохраняет занятость; потерянное подтверждение безопасно повторяется.
7. `internal/observability/migration.go` — пять метрик; action outcome пишется после
   commit, байты — по каждой попытке, остаток бюджета читается при сборе telemetry.
8. `internal/app/migration_provider.go` и `app.go` — сборка клиентов, запуск и
   ожидание остановки воркеров/очистки при `MIGRATION_ENABLED`.
9. Тесты: `internal/service/executor/*_test.go` описывают каждый исход как
   последовательность переходов; `internal/repository/action/*_integration_test.go`
   проверяют блокировки на PostgreSQL. `*stand*_integration_test.go`,
   `failures_integration_test.go`, `restart_integration_test.go` и
   `faults_integration_test.go` — реальные сервисы с точечными обёртками клиентов.
   Полное условие запуска — `SPPR_TEST_MIGRATION_STAND=1` плюс `SPPR_TEST_PG_DSN`;
   без флага стенд не вызывается. После отмены старого Run создаётся новый исполнитель
   на той же schema, а не подставляется готовый running-state.

Для проверки всего пути используй `make smoke-migration` на запущенном SPPR или
`make smoke-migration-local` на отдельном Compose-стенде с короткими согласованными
лимитами. Команда проходит COLD → HOT → COLD, проверяет GET, отсутствие старых blobs
и возврат HOT к исходной занятости. Stand-тест DELETE проверяет отложенную очистку
без ожидания десятиминутной задержки Metadata; источник в тесте сбоя удаления имеет
свой короткий тестовый delay без конкурентных читателей. Полные команды, env,
границы проверки и ручной пример с журналом — в [README](README.md).

Ответь себе: зачем копия проверяется до CAS (Gateway не должен читать
непроверенный blob); почему источник удаляется только после
`MIGRATION_SOURCE_CLEANUP_DELAY > GATEWAY_MAX_READ_DURATION` (GET, начатый до
переключения, ещё читает старый blob); что остаётся после сбоя на каждом шаге в
Metadata (копия `reserved`/`promoted`/`cleanup_pending`), в БД SPPR (`running` с
шагом, открытое списание) и на дисках (частичная или полная копия под
`blob_id` резервирования).

## Planner

Planner строит план, а применяет его только исполнитель, поэтому сначала пойми
раздел выше. Формулы — в тексте ВКР (постановка задачи размещения) и в
[ТЗ этапа 7](../../docs/planner-spec.md).

1. `internal/model/planner.go` — вход алгоритма (`PlanInput`, `PlanItem`,
   блокировки), рекомендация с объяснением, ревизия плана, команда.
2. `internal/service/planner/build.go` — чистая функция `Build`: разгрузка
   переполненного HOT, кандидаты COLD по `Δ/s`, два варианта вытеснения,
   дозаполнение освобождённого места, порог Δmin, выселение старых версий.
   `build_test.go` — случаи по одному, `exact_test.go` — сравнение с перебором.
3. `snapshot.go` — что Planner читает на момент T и как превращает это в
   `PlanInput`: каталог и занятость из Metadata, закрепления, интервал, бюджет,
   принятые действия, минутные агрегаты → признаки → EWMA. `assumeCaughtUp` —
   временное правило полноты до этапа 5.
4. `cycle.go` — таймер и `recompute`, сохранение ревизии одной транзакцией,
   `history_incomplete` и `failed` вместо пустых планов.
5. `commands.go` — идемпотентность по `command_id`, `PLAN_STALE`,
   `DEPENDENCY_REQUIRED`, подтверждение = `SubmitActions` в той же транзакции.
6. `migrations/20261029000000_planner.up.sql`, `internal/repository/plan/` —
   ревизии, рекомендации, зависимости, журнал команд, настройки;
   `internal/repository/history/` — агрегаты и пропуски Ingestor.
7. `internal/handler/planner/`, `internal/handler/access/` — JSON API и место,
   где часть 3 подключит Auth и AuthZ.
8. Связь с исполнителем: зависимое действие ждёт удаления источника
   предшественника (`ListAwaitingSourceCleanup`, `MarkSourceCleaned` в цикле очистки).

Ответь себе: почему самый популярный объект не всегда переносится (большой
размер даёт малый `Δ/s`; штраф P и порог Δmin; бюджет); почему хороший прогноз не
даёт немедленной выгоды (замена K объектов занимает не меньше `2Ks/B` циклов, а
перенос в HOT ждёт удаления вытесненного источника); почему подтверждение части
плана без вытеснения отклоняется, а не дополняется.

## Что учитывать при следующем изменении

Нельзя подтверждать Kafka до commit, обходить `FOR UPDATE` или отдельно
обновлять прогресс и агрегаты. Нельзя считать отсутствие событий нулевым
спросом, пока полнота истории не определена. `last_event_at` одного потока —
не общий watermark W.

Producer Gateway публикует событие из service-слоя: PUT — после регистрации
версии в Metadata, GET — после открытия потока из Storage. `bytes` у GET —
длина отдаваемого объекта: чтение, оборванное на середине передачи, всё равно
считается спросом.

После мержа producer прочитай `services/gateway/internal/service/producer/access`:
номер, marshal и `Send` выполняются под мьютексом потока, поэтому события
попадают в sarama по порядку номеров. Переполнение буфера sarama не заставляет
HTTP ждать Kafka: событие теряется с потраченным номером. Буфер в памяти
теряется при аварии. Гарантии собраны в [спеке](../../docs/kafka-spec.md) и
[PR producer](https://github.com/WithSoull/s3-sppr/pull/1).

После мержа обоих PR для проверки всего пути используй `make smoke-sppr-local` из корня.
Он поднимает Gateway/SPPR и проверяет HTTP → Kafka → PostgreSQL, включая
побайтовый replay и отдельный поток с gap. Интеграционные тесты выше остаются
способом проверить Ingestor без остальных сервисов.

Нет восстановления gaps, контроля потерянного хвоста, retention и DLQ. История пока растёт без ограничения. Это
границы текущего Ingestor, которые нужно учесть до Feature Builder и эксперимента.
