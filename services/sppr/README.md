# SPPR Service

Основа сервиса поддержки принятия решений OpenS3 на Go. Текущий срез — Ingestor
из шага 3.1 (история обращений, дедупликация, контроль пропусков и агрегаты) и
исполнитель переносов HOT/COLD из этапа 4.3: действия, воркеры, бюджет, восстановление после рестарта и очистка blobs.

Для первого чтения кода: [онбординг](ONBOARDING.md). Контракт событий и правила
доставки — [спека Kafka](../../docs/kafka-spec.md).

## Назначение и возможности

- принимает protobuf-события чтения и записи из Kafka topics `object-read` и
  `object-written`;
- сохраняет историю и координаты доставки в PostgreSQL;
- считает спрос только по успешным клиентским чтениям;
- фиксирует пропущенные номера отдельно от нулевого спроса;
- предоставляет health checks, структурированные логи и OTEL metrics;
- принимает через HTTP API действия переноса версии между классами HOT/COLD с
  зависимостями `depends_on`, бюджетом копирования, минимальным интервалом Δmin,
  закреплениями и паузой;
- при `MIGRATION_ENABLED=true` выполняет действия пулом воркеров по протоколу
  Metadata `PlacementService`: резервирование, потоковое копирование между
  Storage, сверка размера и MD5, переключение по CAS; после рестарта продолжает
  начатые действия;
- очищает источники переносов, отменённые цели и копии `orphan` удалённых объектов
  после установленной Metadata задержки, включая повтор после ошибки удаления.

Feature Builder, Predictor и Planner относятся к следующим шагам. Цикл очистки
старых blobs работает в том же процессе; проверки сбоев и `make smoke-migration`
описаны ниже. Planner, прогноз и HTML-панель пока отсутствуют.

## Архитектура и зависимости

```text
Kafka object-read    → go-kit Consumer → Logging → Retry → ObjectReadHandler    ┐
Kafka object-written → go-kit Consumer → Logging → Retry → ObjectWrittenHandler ┤
                                     protobuf decoder → model.AccessEvent ←─────┘
                                                     ↓
                                                Ingestor service
                                                     ↓
                                  Event repository + Transaction manager
                                                     ↓
                                                 PostgreSQL
```

Исполнитель переносов живёт в том же процессе:

```text
HTTP /api/migration/* → handler/migration → service.MigrationService ──┐
                                                                       ├→ action/settings/pin repositories → PostgreSQL
MIGRATION_ENABLED → service.MigrationExecutor (MIGRATION_WORKERS) ─────┘
                    │  ListMigrationCopies, GetPlacement, ReserveCopy,
                    │  SwitchPlacement, AbortCopy
                    ├→ client/metadata → Metadata PlacementService :50052
                    └→ client/storage  → Storage HOT / COLD (RetrieveObject → StoreObject, DeleteObject)
```

Исполнитель читает и пишет Storage напрямую, не через Gateway, поэтому
технические копии не попадают в `object-read` и не искажают спрос. Клиенты
переводят gRPC-коды в sentinel-ошибки `model.Err*`, сервис от gRPC не зависит.

Как в Users, зависимости собирает `internal/app/service_provider.go`: интерфейсы
service/repository, конструкторы и ленивое создание инфраструктуры. Provider
получает конфигурацию явно; ошибки зависимостей возвращаются в startup.
Зависимости разрешаются до запуска goroutines. SQL находится в repository,
валидация события и граница транзакции — в service, разбор protobuf — в
`converter/kafka/decoder`, классификация ошибок — в `service/consumer/access`.
Раскладка Kafka-кода повторяет consumer ChatServer, см. [спеку](../../docs/kafka-spec.md#52-consumer--sppr-по-образцу-chatserver).

Используются `shared/pkg/go-kit`: consumer, middleware `Logging` и `Retry`,
PostgreSQL client, transaction manager, closer, logger, metrics и tracing. На
каждый topic создаётся своя sarama consumer group с общим
`KAFKA_CONSUMER_GROUP_ID`. Штатный consumer продолжает чтение после ошибки
handler, поэтому `Retry` повторяет то же сообщение до успеха или отмены сессии.
Следующий offset не может подтвердиться поверх несохранённого события.
Отклонённое событие handler сам завершает успехом, см. классификацию ниже.

Цикл `MigrationCleanup` независимо от воркеров вызывает Metadata `ListDueCleanups`,
Storage `DeleteObject` и Metadata `CompleteCleanup`. Пауза действий его не останавливает.

SIGINT/SIGTERM отменяет consumers, цикл очистки, воркеры переносов и завершает HTTP-запросы.
Прерванное действие остаётся `running` и продолжается при следующем старте. Затем shared closer
закрывает consumer groups, PostgreSQL, listener и telemetry. Closer закрывает их
параллельно; это безопасно, потому что `Run` уже дождался остановки consumers.
Зависимости регистрируются в closer сразу после создания, поэтому частичный
startup тоже освобождает их. Последним закрывается logger с выгрузкой OTLP-логов,
если она включена.

## Структура проекта

```text
cmd/server/                       entrypoint, flags, signals
cmd/smoke/                        сквозная проверка Gateway → Kafka → SPPR
cmd/smoke-migration/              Gateway → API переносов → Storage → очистка
internal/app/                     lifecycle и DI service provider
internal/config/env/              загрузка и проверка env
internal/converter/kafka/         интерфейсы decoders; decoder/ — protobuf → domain event
internal/service/consumer/access/ consumer на topic, handlers и классификация ошибок
internal/handler/health/          HTTP health checks
internal/handler/migration/       HTTP API переносов: JSON ↔ model, коды ответов
internal/service/ingestor/        validation и транзакция
internal/service/migration/       MigrationService: Submit, отмена, бюджет, настройки, закрепления
internal/service/executor/        воркеры, шаги действия, копирование, восстановление
internal/service/cleanup/         независимый цикл очистки копий из Metadata
internal/client/                  PlacementClient и StorageClient; metadata/, storage/ — gRPC
internal/repository/event/        PostgreSQL queries
internal/repository/action/       действия, зависимости, журнал, списания бюджета, взятие SKIP LOCKED
internal/repository/settings/     бюджет, Δmin, пауза; сумма списаний за окно
internal/repository/pin/          закрепления bucket/key
internal/model/                   события, ErrRejected, действия переноса и ошибки
internal/validator/               проверка контракта
internal/observability/           ingestion и migration metrics
pkg/mocks/                        generated minimock mocks
migrations/                       migrate up/down SQL
```

## API

| HTTP | Ответ |
|---|---|
| `GET /healthz` | 200, пока работает HTTP server |
| `GET /readyz` | 200 при доступной PostgreSQL; иначе 503 |

Readiness не отражает Kafka, Metadata и Storage и не говорит о полноте истории:
остановленная partition видна в логах и по метрике `outcome=error`. Business
gRPC API нет.

### HTTP API переносов

JSON на том же порту, префикс `/api/migration`. Ручки отвечают и при
`MIGRATION_ENABLED=false`: действия можно поставить в очередь до включения
воркеров. Аутентификации нет до этапа 7, `author` — свободный текст, поэтому
Compose публикует порт только на `127.0.0.1`. Planner этапа 7 вызывает те же
методы `service.MigrationService` без HTTP.

| Метод | Путь | Назначение |
|---|---|---|
| POST | `/actions` | `{plan_id, actions:[{action_id, version_id, expected_generation, source_class, target_class, size_bytes, depends_on}]}` → 202 `{accepted, existing}` |
| GET | `/actions?plan_id=&status=&page_size=&page_token=` | страница по `action_id`; `page_size` 1..1000, по умолчанию 100; `next_page_token` — последний `action_id` |
| GET | `/actions/{id}` | действие, `depends_on`, `blocked_by` и журнал `events` |
| POST | `/actions/cancel` | `{plan_id}` или `{action_ids}`; отменяет только `pending` и их зависимые → `{cancelled, dependents_cancelled}` |
| POST | `/pause`, `/resume` | новые действия не берутся; начатые доделываются; очистка продолжается |
| GET / PUT | `/budget` | `{bytes, window_seconds}`; GET также `spent`, `remaining` за скользящее окно |
| GET / PUT | `/settings` | `{min_interval_seconds}`; GET также `paused` (только чтение) |
| GET / POST / DELETE | `/pins` | `{bucket, key, reason, author}`; DELETE по `?bucket=&key=`, ключ может содержать `/` |

Ошибки — `{"error": "..."}`: 400 валидация и неизвестные поля JSON, 404 нет
действия или закрепления, 409 `action_id` уже принят с другими полями или
закрепление существует, 500 без деталей (детали в логе).

`action_id` — UUID от вызывающего, ключ идемпотентности и `migration_id` в
Metadata. Повтор с теми же полями возвращает его в `existing`; с другими полями
весь запрос отклоняется с 409. `depends_on` указывает на действия того же
запроса или уже принятые этого плана, без циклов. Действие, чей предшественник
уже `failed`/`cancelled`, сразу отменяется с `dependency_failed`.

| Статус | Значение |
|---|---|
| `pending` | ждёт; `blocked_by`: `paused`, `dependencies`, `budget` или пусто (будет взято) |
| `running` | у воркера; `step`: `reserving`, `copying`, `switching`, `aborting` |
| `succeeded` | расположение переключено |
| `failed` | `reason`: `stale`, `no_capacity`, `verify_mismatch`, `source_missing`, `upstream_unavailable`, `internal` |
| `cancelled` | `reason`: `user`, `pinned`, `min_interval`, `dependency_failed` |

Шаг `aborting` добавлен к шагам ТЗ: причина отказа записывается до вызова
`AbortCopy`, поэтому рестарт доводит отмену с той же причиной, а не копирует заново.

Один topic — одно proto-сообщение из
[`shared/api/events/v1/access.proto`](../../shared/api/events/v1/access.proto):
`object-read` несёт `events.v1.ObjectRead`, `object-written` —
`events.v1.ObjectWritten`. Decoder переводит оба в `model.AccessEvent`: операцию
задаёт topic, `result` всегда `success`, потому что Gateway публикует только
успешные операции. Проверка контракта — `internal/validator/event.go`.

| Поле | Правило |
|---|---|
| `position.gateway_id` | стабильный идентификатор экземпляра Gateway |
| `position.stream_id` | новый UUID на каждый старт Gateway и на каждый topic |
| `position.sequence` | int64 от 1 внутри потока |
| `occurred_at` | `google.protobuf.Timestamp`, не убывает внутри потока; хранится в UTC с микросекундной точностью |
| `bucket`, `object_key` | непустые UTF-8 строки, максимум 255 и 1024 байта |
| `version_id` | обязателен, максимум 256 байт |
| `origin` | `ORIGIN_CLIENT` или `ORIGIN_MIGRATION`; `ORIGIN_UNSPECIFIED` отклоняется |
| `bytes` | длина отданного диапазона или размер объекта, неотрицательный int64 |

`gateway_id` и `stream_id` соответствуют `[A-Za-z0-9_.-]{1,128}`. Kafka key равен
`gateway_id/stream_id`, поэтому весь поток попадает в одну partition. У каждого
topic свой поток и свой счётчик: общий номер дал бы ложные пропуски, потому что
Kafka не упорядочивает разные topics. Поток принадлежит одному topic/partition;
событие из другой partition отклоняется. Число partitions не меняется: после
расширения topic hash key указал бы на другую partition.
Gateway producer поставляется в независимом [PR #1](https://github.com/WithSoull/s3-sppr/pull/1)
и публикует успешные GET/PUT при `ACCESS_EVENTS_ENABLED=true`. Ingestor можно
запускать и тестировать до мержа producer: достаточно Kafka и PostgreSQL.

## Данные и основные сценарии

Одна транзакция PostgreSQL выполняет следующие действия:

1. Создаёт и блокирует строку потока `(gateway_id, stream_id)`.
2. Сверяет повтор по идентификатору и SHA-256 исходного значения Kafka
   (`msg.Value`). Идентичный повтор ничего не меняет; другой payload под тем же
   номером отклоняется.
3. Сохраняет событие и диапазон пропущенных номеров.
4. Для успешного клиентского чтения обновляет минутный агрегат.
5. Продвигает номер и время потока. После commit handler возвращает успех,
   shared consumer вызывает `MarkMessage`.

Сбой между commit и Kafka offset commit приводит к повторной доставке и
дедупликации. `FOR UPDATE` и уникальные ключи защищают от конкурентных повторов.
При ошибке вся транзакция откатывается.

| Таблица | Назначение |
|---|---|
| `access_events` | исходные операции, время, версия, результат, объём, hash, Kafka coordinates |
| `ingestion_streams` | последний номер/время, начало наблюдения, сумма пропусков |
| `ingestion_gaps` | отсутствующие номера и консервативные временные границы |
| `read_aggregates` | число чтений и байты по версии и минуте |

`last_event_at` — прогресс потока, а не глобальный watermark W. На шаге 3.2
потребуются учёт живых Gateway, heartbeat и завершённых потоков. Потерянный хвост
без следующего события пока не обнаруживается. Первый номер больше 1 создаёт
пропуск с неизвестной нижней границей времени. Старые пропуски автоматически не
закрываются; позднее событие из пропуска отклоняется.

Handler делит ошибки на два класса. Отклонения repository и service помечают
ошибкой `model.ErrRejected` (`errors.Is`):

| Класс | Примеры | Действие |
|---|---|---|
| Инфраструктурная | БД недоступна, таймаут, сбой commit | ошибка возвращается в `Retry`; partition ждёт и повторяет через `KAFKA_RETRY_INTERVAL`, offset не пропускается |
| Отклонённое событие | не декодируется protobuf, не проходит валидацию, позднее событие из пропуска, тот же номер с другим payload, время назад, поток сменил topic/partition | лог `Access event rejected` и метрика `outcome=rejected`; handler возвращает `nil`, offset подтверждается |

Отклонённое событие не попадает в историю. Его номер виден как пропуск, когда
придёт следующее событие потока, а partition не блокируется навсегда. DLQ нет.

### Перенос версии

| Таблица | Назначение |
|---|---|
| `migration_actions` | поля действия, статус, шаг, причина, число попыток, `bytes_charged`, времена |
| `migration_action_deps` | рёбра `depends_on` |
| `migration_action_events` | журнал: время, из какого в какой статус/шаг, байты, причина, ошибка |
| `migration_budget_ledger` | одна строка на попытку копирования: открыта с плановым размером, закрыта фактическими байтами |
| `migration_settings` | одна строка: бюджет (байт за окно), Δmin, пауза; при первом старте из env |
| `migration_pins` | закрепления bucket/key — «не переносить» |

Воркер берёт действие в одной транзакции: блокирует строку `migration_settings`
(конкурентные взятия идут по очереди и не превышают бюджет), считает списания за
окно и выбирает самое старое `pending`, все зависимости которого `succeeded`, а
размер помещается в остаток, с `FOR UPDATE SKIP LOCKED`. В той же транзакции
открывается списание `size_bytes`, действие становится `running/reserving`.
Затем воркер:

1. Спрашивает Metadata `ListMigrationCopies(action_id)`. Пусто — новое действие.
2. `GetPlacement(version_id)`: ключ закреплён → `cancelled/pinned`; `generation > 1`
   и с последнего переключения прошло меньше Δmin → `cancelled/min_interval`. Первое
   расположение выбрано при записи, его время Δmin не ограничивает.
3. `ReserveCopy(migration_id = action_id, …)`: `ABORTED` или нет версии →
   `failed/stale`, `RESOURCE_EXHAUSTED` → `failed/no_capacity`. Во всех исходах
   шагов 2–3 списание возвращается: байты не передавались.
   Перед копированием (в том числе при восстановлении) размер резервирования из
   Metadata должен совпадать с плановым `size_bytes`: иначе `AbortCopy` →
   `failed/stale`, с обоими размерами в журнале ошибки и возвратом списания за
   неначатую попытку; списание прерванной ранее попытки сохраняется.
4. `copying`: поток `RetrieveObject` из Storage источника идёт в `StoreObject`
   Storage цели с `blob_id` резервирования и размером в header; по пути считаются
   байты и MD5. Списание закрывается фактически переданными байтами.
5. Размер и MD5 источника сверяются с ответом цели; копия проверяется до CAS,
   чтобы Gateway никогда не читал непроверенный blob. Не совпало → `AbortCopy` →
   `failed/verify_mismatch`.
6. `switching`: `SwitchPlacement` с `source_cleanup_delay = MIGRATION_SOURCE_CLEANUP_DELAY`.
   `ABORTED` → `AbortCopy` → `failed/stale`.
7. `succeeded`.

`UNAVAILABLE`/`DEADLINE_EXCEEDED` повторяются с экспоненциальной паузой от
`MIGRATION_RETRY_BACKOFF` до `MIGRATION_MAX_ATTEMPTS` попыток; затем `AbortCopy` и
`failed/upstream_unavailable`. Каждая попытка копирования списывается отдельно:
неудачная остаётся списанной, следующая открывает новое списание. Поэтому повторы
могут превысить бюджет окна на одну попытку. Нет blob источника → `failed/source_missing`.
Если `AbortCopy` отвечает `FAILED_PRECONDITION`, переключение уже произошло (ответ
`SwitchPlacement` потерялся), и действие становится `succeeded`. При переходе в
`failed` или `cancelled` все `pending` действия, транзитивно зависящие от него,
становятся `cancelled/dependency_failed` в той же транзакции. ТЗ требует этого
только для `failed`; отмена предшественника тоже каскадируется, иначе зависимые
ждали бы вечно.

Источник не удаляется сразу: GET, начавшийся до переключения, ещё читает старый
blob. Поэтому `MIGRATION_SOURCE_CLEANUP_DELAY` должна превышать
`GATEWAY_MAX_READ_DURATION`, иначе SPPR не стартует.

**Восстановление.** Воркер сначала берёт `running` действия, которые этот процесс
не обрабатывает (после рестарта — все), даже на паузе, и продолжает их по
состоянию копий в Metadata:

| Копии `action_id` в Metadata | Действие |
|---|---|
| нет | шаги 2–7; `ReserveCopy` идемпотентен |
| цель `reserved` | заново `GetPlacement`, копирование в тот же `blob_id` (запись заменяет файл), сверка, переключение; прерванная попытка остаётся списанной по плановому размеру |
| цель `promoted` | `succeeded` |
| цель отменена (`cleanup_*`, `deleted`) | `failed` с причиной, сохранённой до `AbortCopy`, иначе `internal` |
| шаг `aborting` | повтор `AbortCopy`, затем `failed` с сохранённой причиной |

Ошибка записи в БД SPPR оставляет действие `running`; его возьмёт следующий
свободный воркер. Протокол Metadata идемпотентен, поэтому повтор шага безопасен.

### Очистка blobs

При `MIGRATION_ENABLED=true` цикл выполняет проход сразу при старте, затем каждые
`MIGRATION_CLEANUP_INTERVAL` (30s). Проход получает до 100 копий из Metadata;
обрабатываются все роли: `source`, `target` и `orphan`, без требования `migration_id`.
Для `orphan` он пуст. Metadata выбирает только `cleanup_pending` и `cleanup_failed`
с наступившим `cleanup_after`.

Удаление идёт напрямую в Storage класса копии, с таймаутом 5s на RPC. Отсутствующий
blob считается успешно удалённым. После успеха `CompleteCleanup(deleted=true)`
освобождает учтённое место; при ошибке удаления `deleted=false` с текстом ошибки
оставляет место занятым и переводит копию в `cleanup_failed`. Ошибка одной копии
не мешает остальному пакету. Если ответ `CompleteCleanup` потерян, следующий проход
безопасно повторяет идемпотентное удаление. Отмена процесса оставляет работу следующему
старту. Пауза переносов не участвует в этом алгоритме.

Источники ждут `MIGRATION_SOURCE_CLEANUP_DELAY`; копии удалённых объектов —
`METADATA_DELETE_CLEANUP_DELAY` в Metadata. Обе задержки должны превышать общий
`GATEWAY_MAX_READ_DURATION`. Отменённые цели могут очищаться сразу после `AbortCopy`.

## Конфигурация

Окружение имеет приоритет над необязательным `.env`. Флаг `-config-path` задаёт
путь. Полный пример — `.env.example`.

| Переменные | Default / требование |
|---|---|
| `PG_DSN` | предпочтительный способ задать подключение |
| `PG_HOST`, `PG_PORT_INNER`, `PG_DATABASE_NAME`, `PG_USER`, `PG_PASSWORD` | все обязательны без `PG_DSN` |
| `PG_SSL_MODE`, `PG_TIMEOUT`, `PG_LOGGING` | `disable`, `5s`, `false` |
| `KAFKA_BOOTSTRAP` | обязательный список `host:port` через запятую |
| `KAFKA_OBJECT_READ_TOPIC`, `KAFKA_OBJECT_WRITTEN_TOPIC` | `object-read`, `object-written` |
| `KAFKA_CONSUMER_GROUP_ID` | `sppr`; общая группа для обоих topics |
| `KAFKA_RETRY_INTERVAL` | `1s`, положительный |
| `HTTP_ADDRESS`, `SHUTDOWN_TIMEOUT` | `0.0.0.0:8086`, `15s` |
| `LOGGER_LEVEL`, `LOGGER_AS_JSON`, `LOGGER_ENABLE_OLTP` | задать явно; пример `info`, `true`, `false` |
| `OTEL_SERVICE_NAME`, `OTEL_SERVICE_VERSION`, `OTEL_ENVIRONMENT` | задать явно |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | задать явно, gRPC collector `host:port` |
| `OTEL_METRICS_PUSH_TIMEOUT` | задать явно; интервал экспорта, пример `10s` |
| `MIGRATION_ENABLED` | `false`; запускать воркеры и цикл очистки. API отвечает и при `false` |
| `MIGRATION_CLEANUP_INTERVAL` | `30s`; положительный интервал проходов очистки |
| `MIGRATION_WORKERS` | `2`; параллельные действия |
| `MIGRATION_MAX_ATTEMPTS`, `MIGRATION_RETRY_BACKOFF` | `5`, `1s`; попытки при `UNAVAILABLE`/`DEADLINE_EXCEEDED`, пауза удваивается |
| `MIGRATION_SOURCE_CLEANUP_DELAY` | `10m`; `cleanup_after` источника, строго больше `GATEWAY_MAX_READ_DURATION` |
| `GATEWAY_MAX_READ_DURATION` | `5m`; тот же параметр, что у Gateway, Compose передаёт одну переменную |
| `MIGRATION_BUDGET_BYTES`, `MIGRATION_BUDGET_WINDOW` | `1GiB` (байты или суффикс `KiB`/`MiB`/`GiB`/`TiB`), `1h`; начальный бюджет |
| `MIGRATION_MIN_INTERVAL` | `1h`; начальное Δmin |
| `METADATA_GRPC_ADDR`, `METADATA_RPC_TIMEOUT` | `localhost:50052`, `5s` |
| `STORAGE_HOT_GRPC_ADDR`, `STORAGE_COLD_GRPC_ADDR` | `localhost:50056`, `localhost:50053`, как у Gateway |

Параметры sarama — константы `config/env/sarama.go`: `OffsetOldest`,
`Consumer.Return.Errors=true` (ошибки группы читаются и логируются). Отдельного
лимита на попытку нет: `ExecContext` shared PG client ограничен `PG_TIMEOUT`.
Без `PG_DSN` компоненты подключения кодируются как PostgreSQL URL: пробелы и
спецсимволы в пароле не меняют смысл остальных параметров.

Бюджет, окно и Δmin из env записываются только при первом старте; дальше
действуют значения, заданные через API. Окно и Δmin хранятся в целых секундах.
Настройки проверяются и при выключенном исполнителе.

Compose использует отдельную development БД на порту 5436, Kafka `kafka:29092`,
HTTP `127.0.0.1:8086`, collector `otel-collector:4317`, Metadata и Storage по
именам сервисов. Collector запускается отдельно. При
его отсутствии ingestion работает, exporter сообщает об ошибках. Логи по
умолчанию идут в stdout; OTLP-экспорт логов отключён.

## Запуск

Из корня репозитория:

```bash
make up-sppr
curl -f http://localhost:8086/readyz
docker compose logs -f sppr
```

Поднимаются SPPR, отдельный PostgreSQL, `migrator-sppr`, Kafka и ZooKeeper.
Gateway и остальные прикладные сервисы не требуются. Topics `object-read` и
`object-written` broker создаёт автоматически при текущих development настройках;
иначе создайте их заранее.

После мержа обоих независимых PR доступен весь контур с реальным Gateway.
Ниже дополнительные команды приёмки; автономный Ingestor от них не зависит:

```bash
make up-sppr-gateway
make smoke-sppr
# Сборка, запуск и проверка одной командой:
make smoke-sppr-local
```

Smoke создаёт уникальный bucket и оставляет его небольшие объекты для диагностики.
Проверяются обычный/пустой/chunked PUT/GET, версия, реальный объём, отсутствие
события у GET несуществующего объекта, побайтовый повтор доставленного сообщения
без роста спроса и gap в отдельном потоке `migration`. DSN берётся из `SPPR_SMOKE_PG_DSN`; Makefile по умолчанию
использует development PostgreSQL на 5436. Адреса можно переопределить:

```bash
SPPR_SMOKE_PG_DSN='postgres://user:password@localhost:5436/test_db?sslmode=disable' \
go run ./services/sppr/cmd/smoke -gateway http://127.0.0.1:8080 \
  -metadata 127.0.0.1:50052 -kafka localhost:9092 -object-read-topic object-read
```

Запуск Go-процесса с хоста:

```bash
docker compose --profile sppr up -d postgres-sppr kafka
docker compose --profile sppr run --rm migrator-sppr
cd services/sppr
cp .env.example .env
go run ./cmd/server -config-path .env
```

Исполнитель переносов вместе с Gateway, Metadata и обеими Storage:

```bash
make up-sppr-migration
curl -s http://localhost:8086/api/migration/budget
make smoke-migration SMOKE_MIGRATION_ARGS="-timeout 12m"
```

Миграции выполняет `migrate/migrate:v4.19.1` отдельно от приложения. Приложение
проверяет таблицы перед запуском consumer. Формат миграций этого сервиса —
`<version>_<name>.up.sql` / `.down.sql`. Не запускайте их через Goose: другие
сервисы используют другой формат. Повторное `up` без новых миграций безопасно.

## Примеры использования

События — бинарный protobuf, поэтому `kafka-console-producer` для ручной
отправки не подходит. Последовательность `1, 1, 3` с повтором и пропуском
воспроизводит [Kafka-тест приложения](internal/app/app_integration_test.go); с
реальным Gateway события создаёт `make smoke-sppr-local`. Посмотреть результат в
development БД из корня репозитория:

```bash
docker compose exec postgres-sppr psql -U sppr_user -d sppr_db -c 'TABLE read_aggregates; TABLE ingestion_gaps;'
```

Перенос одной версии COLD → HOT. Версию и поколение показывает Metadata:

```bash
grpcurl -plaintext -d '{"bucket_name":"docs-demo","key":"example.txt"}' \
  localhost:50052 opens3.metadata.v1.MetadataService/GetObjectMeta
# versionId, placementGeneration, storageClass, sizeBytes

curl -s -X POST localhost:8086/api/migration/actions -d '{
  "plan_id": "8a4d5f8e-6f1e-4c55-9d0b-5f2b9c3e0a11",
  "actions": [{"action_id": "3f0c9b8e-2f6b-4b1e-9d55-0e7c1a2b3c4d",
    "version_id": "<versionId>", "expected_generation": 1,
    "source_class": "COLD", "target_class": "HOT", "size_bytes": <sizeBytes>, "depends_on": []}]}'
# {"accepted":["3f0c9b8e-..."],"existing":[]}

curl -s localhost:8086/api/migration/actions/3f0c9b8e-2f6b-4b1e-9d55-0e7c1a2b3c4d | jq '{status, step, reason, events}'
curl -s -X PUT localhost:8086/api/migration/budget -d '{"bytes":5368709120,"window_seconds":3600}'
curl -s -X POST localhost:8086/api/migration/pins -d '{"bucket":"docs-demo","key":"example.txt","reason":"demo","author":"ops"}'
```

### Ручная демонстрация COLD → HOT → COLD

Нужны `curl`, `grpcurl`, `jq`, `uuidgen` и включённый исполнитель. Пример запускается
в subshell, сохраняет Δmin и восстанавливает его при выходе. Уменьшение Δmin —
настройка демонстрационного стенда; задержки безопасной очистки остаются прежними.

```bash
(
set -eu
api=http://localhost:8086/api/migration
bucket="migration-demo-$(date +%s)"
owner=$(uuidgen)
plan=$(uuidgen)
grpcurl -plaintext -d "$(jq -nc --arg name "$bucket" --arg owner "$owner" '{name:$name,owner_id:$owner}')" \
  localhost:50052 opens3.metadata.v1.MetadataService/CreateBucket
curl -fsS -X PUT --data-binary 'migration demo' "http://localhost:8080/objects/$bucket/demo.txt"
old_min=$(curl -fsS "$api/settings" | jq -r .min_interval_seconds)
trap 'curl -fsS -X PUT "$api/settings" -d "{\"min_interval_seconds\":$old_min}" >/dev/null' EXIT
curl -fsS -X PUT "$api/settings" -d '{"min_interval_seconds":0}'
for target in HOT COLD; do
  placement=$(grpcurl -plaintext -d "$(jq -nc --arg b "$bucket" '{bucket_name:$b,key:"demo.txt"}')" \
    localhost:50052 opens3.metadata.v1.MetadataService/GetObjectMeta)
  action=$(uuidgen)
  jq -nc --arg plan "$plan" --arg action "$action" --arg target "$target" --argjson p "$placement" \
    '{plan_id:$plan,actions:[{action_id:$action,version_id:$p.versionId,expected_generation:($p.placementGeneration|tonumber),source_class:$p.storageClass,target_class:$target,size_bytes:($p.sizeBytes|tonumber),depends_on:[]}]}' \
    | curl -fsS -X POST "$api/actions" -H 'Content-Type: application/json' --data-binary @-
  done_action=false
  for attempt in $(seq 1 120); do
    state=$(curl -fsS "$api/actions/$action")
    case $(printf '%s' "$state" | jq -r .status) in
      succeeded) done_action=true; break ;;
      failed|cancelled) printf '%s' "$state" | jq .; exit 1 ;;
    esac
    sleep 1
  done
  "$done_action" || { printf '%s' "$state" | jq .; exit 1; }
  curl -fsS "$api/actions/$action" | jq '{action_id,status,reason,bytes_charged,events}'
  curl -fsS "http://localhost:8080/objects/$bucket/demo.txt"
done
grpcurl -plaintext localhost:50052 opens3.metadata.v1.PlacementService/ListStorageClasses
)
```

После второго переноса HOT ещё занят источником до `cleanup_after`; после очистки
занятость уменьшится. Отслеживать её можно повторным `ListStorageClasses`, копии
конкретного действия — `ListMigrationCopies` с его `migration_id=action_id`.

### Smoke переносов

`make smoke-migration` требует уже запущенных Gateway, Metadata, обеих Storage и
SPPR с `MIGRATION_ENABLED=true`. Команда создаёт уникальный bucket, делает PUT,
переносит одну версию COLD → HOT → COLD через HTTP API и проверяет GET, идентичность
версии, рост generation ровно на один на каждый перенос, очистку обоих источников,
`NOT_FOUND` при прямом чтении удалённых blobs и возврат суммарной занятости HOT
(`placed_bytes + copy_bytes`) к исходной.

Команда временно ставит Δmin=0 через API и восстанавливает прежнее значение при
выходе. Пауза должна быть снята заранее; нужен бюджет для обоих переносов. Настройки
бюджета команда не меняет. Объект в COLD остаётся для диагностики. Используйте
отдельный стенд без параллельных записей и очистки старых тестовых данных:
изменения чужих объектов нарушают сравнение глобальной занятости.

С defaults очистка источника занимает 10 минут; таймаут smoke по умолчанию — 12 минут. Для
быстрой демонстрации на своём Compose-стенде:

```bash
make smoke-migration-local
# Эквивалент настройки перед запуском:
GATEWAY_MAX_READ_DURATION=2s MIGRATION_SOURCE_CLEANUP_DELAY=5s \
METADATA_DELETE_CLEANUP_DELAY=5s MIGRATION_CLEANUP_INTERVAL=250ms make up-sppr-migration
make smoke-migration
# Другие адреса и таймаут:
make smoke-migration SMOKE_MIGRATION_ARGS='-sppr http://localhost:8086 -gateway http://localhost:8080 -metadata localhost:50052 -storage-hot localhost:50056 -storage-cold localhost:50053 -timeout 12m'
```

Сокращать только задержку очистки нельзя: SPPR и Metadata откажутся стартовать,
если она не превышает лимит чтения Gateway. `MIGRATION_MIN_INTERVAL` из env
применяется только при первом создании настроек; smoke поэтому использует API.
`make smoke-sppr` отдельно проверяет ingestion; `make smoke-gateway` отдельно
проверяет протокол Metadata и маршрутизацию без зависимости от SPPR.

## Наблюдаемость и диагностика

| Инструмент | Содержимое |
|---|---|
| `sppr_ingestion_events_total` | попытки с `outcome=stored`, `duplicate`, `rejected`, `error` |
| `sppr_ingestion_missing_events_total` | обнаруженные пропущенные номера |
| `sppr_ingestion_duration_seconds` | длительность попытки |
| structured logs | `Access event rejected`, ошибки ingestion перед retry, ошибки consumer group; topic, partition, offset; `Kafka msg received` на каждое сообщение |
| structured logs переносов | `migration action claimed`, `migration action transition` (`action_id`, `version_id`, из/в статус/шаг, причина, ошибка, отменённые зависимые), `migration step failed` |
| `sppr_migration_actions_total{outcome,reason}` | зафиксированные терминальные переходы, включая ручную и каскадную отмену; success имеет пустой reason |
| `sppr_migration_bytes_copied_total{target_class}` | прочитанные для передачи байты каждой попытки, включая неуспешные; не резерв бюджета |
| `sppr_migration_budget_remaining_bytes` | текущий остаток по настройкам и ledger за скользящее окно, обновляется при сборе метрик и на паузе |
| `sppr_migration_cleanups_total{outcome}` | попытки: `deleted`, `delete_failed`, `completion_failed` |
| `sppr_migration_duration_seconds` | время от сохранённого `started_at` до терминального перехода начатого действия, включая простой при рестарте; отменённые до claim не входят |
| cleanup logs | copy_id, blob_id, storage_class, role, outcome, ошибки удаления и подтверждения |
| журнал действия | `GET /api/migration/actions/{id}` → `events` |

Метрики — telemetry процесса, а не долговечный аудит: авария между commit и
записью счётчика может потерять наблюдение. При оборванной передаче счётчик содержит
только байты, увиденные этим процессом; ledger сохраняет принятую в части 2
консервативную оценку прерванной попытки. Ошибка `CompleteCleanup` не освобождает
место до подтверждения Metadata. Детали проверяйте в журнале и `ListStorageClasses`.
В labels нет bucket/key, action_id или version_id; идентификаторы остаются в логах.

Действие, долго стоящее в `running/aborting`, ждёт Metadata: `AbortCopy` повторяется
после каждой паузы. `pending` с `blocked_by=budget` стартует, когда старые
списания выйдут из окна или бюджет увеличат через `PUT /budget`.

Retries увеличивают число попыток с `outcome=error`, но не число чтений в БД.
Object key и Gateway ID не используются как labels. Растущий `outcome=error`
означает, что partition ждёт: проверьте логи и БД. Растущий `outcome=rejected`
означает ошибку producer; такие события видны как пропуски. Tracer
инициализируется, но consumer spans не создаёт: Gateway не передаёт trace context
в Kafka.

## Разработка и тесты

```bash
make test-sppr
SPPR_TEST_PG_DSN='postgres://user:password@localhost:5436/test_db?sslmode=disable' make test-sppr-integration
SPPR_TEST_PG_DSN='postgres://user:password@localhost:5436/test_db?sslmode=disable' SPPR_TEST_KAFKA_BOOTSTRAP=localhost:9092 make test-sppr-integration
cd services/sppr
go generate ./pkg/mocks
go vet ./...
```

Для `go generate ./pkg/mocks` нужен `minimock` v3.4.7 в `PATH`; исходные
директивы также закрепляют версию зависимостью сервиса.

Integration tests создают и удаляют уникальные PostgreSQL schemas; нужны права
`CREATE SCHEMA`. Полный тест также создаёт/удаляет уникальные Kafka topics и
поднимает локальный gRPC OTLP receiver. Без `SPPR_TEST_KAFKA_BOOTSTRAP`
пропускается только Kafka smoke; PostgreSQL tests требуют `SPPR_TEST_PG_DSN`.
Используйте тестовые зависимости.

Workflow `.github/workflows/sppr-ci.yml` запускает vet и весь набор с `-race`
на PostgreSQL 16 и Kafka 7.7.0 при изменениях сервиса или go-kit.
Дополнительный job сквозного smoke выполняется только при наличии Gateway
producer в checkout. В независимом PR Ingestor он пропускает запуск стенда;
после объединения обоих компонентов выполняет `make smoke-sppr-local`.

Unit-тесты исполнителя проверяют каждый исход шагов 2–6, возврат и
корректировку списаний, повторы, потерянный ответ `SwitchPlacement` и
восстановление с каждого шага (`internal/service/executor`). Тесты сервиса и
handler — идемпотентность и 409 у Submit, валидацию, `blocked_by`, каскад при
отмене, коды HTTP. Integration-тесты repository на PostgreSQL проверяют, что
`SKIP LOCKED` не отдаёт действие двум воркерам, зависимое не берётся до
`succeeded` предшественника, конкурентные взятия не превышают бюджет и каждая
попытка копирования списывается.

Проверяются дубли, конкуренция, пропуски, restart service layer после commit,
атомарный rollback, новый stream после рестарта, отклонения с `ErrRejected`,
decoders и классификация ошибок в handlers. Kafka-тест приложения проверяет
повтор, gap, событие второго topic, ожидание partition при сбое БД и
восстановление, подтверждение недекодируемого события без блокировки
следующего, OTEL metrics/logs и shutdown.

### Проверки сбоев на стенде

Обычные `make test-sppr` и `make test-sppr-integration` не обращаются к стенду без
`SPPR_TEST_MIGRATION_STAND=1`. Для полного набора нужны Gateway, Metadata, обе
Storage (`make up-gateway`) и тестовая PostgreSQL с правом `CREATE SCHEMA`.
Metadata должен быть собран из текущего кода с применёнными миграциями части 1
`20261027000000` и `20261027000100`: старый контейнер теряет учёт orphan при DELETE,
что проверяет stand-тест.
SPPR-контейнер с исполнителем/очисткой должен быть остановлен: исполнитель живёт
в самом тесте. Kafka env для этого набора не требуется.

```bash
SPPR_TEST_PG_DSN='postgres://user:password@localhost:5434/test_db?sslmode=disable' \
SPPR_TEST_MIGRATION_STAND=1 make test-sppr-integration
# Только сценарии стенда, с подробным выводом:
SPPR_TEST_PG_DSN='postgres://user:password@localhost:5434/test_db?sslmode=disable' \
SPPR_TEST_MIGRATION_STAND=1 go test -race -tags=integration ./services/sppr/internal/service/executor -run TestStand -v -count=1
```

| Env | Default |
|---|---|
| `SPPR_TEST_MIGRATION_STAND` | выключен; только `1` включает стенд |
| `SPPR_TEST_PG_DSN` | обязателен при включённом стенде; ошибка конфигурации или подключения проваливает тест |
| `SPPR_TEST_GATEWAY_URL` | `http://localhost:8080` |
| `SPPR_TEST_METADATA_GRPC_ADDR` | `localhost:50052` |
| `SPPR_TEST_STORAGE_HOT_GRPC_ADDR` | `localhost:50056` |
| `SPPR_TEST_STORAGE_COLD_GRPC_ADDR` | `localhost:50053` |

Каждый сценарий создаёт свою SPPR schema и уникальный bucket. Schema удаляется;
объекты и blobs стенда остаются для диагностики. Не запускайте набор параллельно
с другими изменениями занятости классов или активной очисткой SPPR.

Проверяются все сценарии части 3: повреждение после хеширования источника,
недоступная цель (ноль и частично переданные байты), отказ `ReserveCopy` из-за
ёмкости с возвратом бюджета и каскадом отмены, overwrite/DELETE во время копирования,
GET во время switch, pin после Submit до claim, Δmin, истечение окна бюджета,
ошибка удаления источника с последующим успехом на паузе. Отказ Storage после
передачи сохраняет списание; это отличается от отказа резервирования Metadata.

Рестарт — отмена и полная остановка одного `Run`, затем **новый** экземпляр на той
же schema и реальных Metadata/Storage. Барьеры стоят до резервирования, после
резервирования до записи шага, внутри copying, до switch и после commit switch
до возврата ответа. Проверяются один target/blob_id, одно увеличение generation,
один терминальный успех и корректный GET.

Metadata стенда может иметь стандартный `METADATA_DELETE_CLEANUP_DELAY=10m`.
DELETE-тест не ждёт физического удаления orphan: проверяет сохранение занятости,
перенос размера из placements в copies, доступность старого blob и отсутствие его
в due-списке до задержки. У теста ошибки удаления источника свой изолированный
объект без конкурентных читателей и тестовый `SourceCleanupDelay=50ms` в конфигурации
экземпляра исполнителя; только там проверяются реальный due, `cleanup_failed`,
повтор, физическое удаление и освобождение места. Настройки работающих контейнеров
не меняются. Тестовая обёртка списка очистки выбирает только собственный copy_id;
чужие blobs не удаляются. Unit-тесты отдельно проверяют роли source/target/orphan,
ошибку подтверждения после успешного удаления, отмену и повтор отсутствующего blob.

## Ограничения и дальнейшая работа

Gateway producer поставляется отдельным PR. Полнота истории ещё не определяется: нет
общего watermark и heartbeat. Буфер sarama в Gateway находится в памяти;
переполнение тратит sequence и приводит к gap при следующем доставленном событии.
Потерянный хвост потока при аварии Gateway не обнаруживается без heartbeat.
Таблицы будущих моделей/планов ещё не создаются. На шаге 3.2 нужны
признаки без утечки будущего, часовые метки и правила полноты. Минутного
агрегата недостаточно для произвольной границы T; точные события сохраняются.

Нет retention/partitioning истории, DLQ, восстановления пропусков, SASL/TLS
конфигурации Kafka и административного доступа. Запуск рассчитан на один
экземпляр во внутренней development сети: несколько исполнителей на одной БД не
поддерживаются, кроме защиты `SKIP LOCKED` при взятии. Прогноз ещё не выполняется;
производительность и квота 2 ядра / 2 ГиБ пока не подтверждены измерениями.

Очистка работает только при `MIGRATION_ENABLED=true`: выключенный процесс или
недоступный Storage оставляет копии занимающими место. Нет
Planner, прогноза, HTML-панели и авторизации API (этапы 6–7). Длительность
одного потока копирования не ограничена отдельным таймаутом.

## Связанные документы

- [Онбординг](ONBOARDING.md) — порядок чтения, алгоритм и ручная проверка.
- [Спека Kafka](../../docs/kafka-spec.md) — контракт, доставка и отказоустойчивость.
- [Gateway](../gateway/README.md) — HTTP PUT/GET.
- [Producer, PR #1](https://github.com/WithSoull/s3-sppr/pull/1) — отдельная публикация обращений.
- [Users](../users/README.md) — образец слоёв и DI.
- [Metadata](../metadata/README.md) — существующие Kafka producers; протокол `PlacementService` и копии для очистки.
- [Storage](../storage/README.md) — `StoreObject` с заданным `blob_id`.
- [Правила разработки](../../AGENTS.md).
