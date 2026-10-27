# SPPR Service

Основа сервиса поддержки принятия решений OpenS3 на Go. Текущий срез — Ingestor
из шага 3.1: история обращений, дедупликация, контроль пропусков и агрегаты.

Для первого чтения кода: [онбординг](ONBOARDING.md). Контракт событий и правила
доставки — [спека Kafka](../../docs/kafka-spec.md).

## Назначение и возможности

- принимает protobuf-события чтения и записи из Kafka topics `object-read` и
  `object-written`;
- сохраняет историю и координаты доставки в PostgreSQL;
- считает спрос только по успешным клиентским чтениям;
- фиксирует пропущенные номера отдельно от нулевого спроса;
- предоставляет health checks, структурированные логи и OTEL metrics.

Feature Builder, Predictor, Planner и переносы относятся к следующим шагам.
Таблицы моделей, планов и миграций пока не создаются: их схема должна
соответствовать реализованным сценариям.

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

SIGINT/SIGTERM отменяет consumers и завершает HTTP-запросы. Затем shared closer
закрывает consumer groups, PostgreSQL, listener и telemetry. Closer закрывает их
параллельно; это безопасно, потому что `Run` уже дождался остановки consumers.
Зависимости регистрируются в closer сразу после создания, поэтому частичный
startup тоже освобождает их. Последним закрывается logger с выгрузкой OTLP-логов,
если она включена.

## Структура проекта

```text
cmd/server/                       entrypoint, flags, signals
cmd/smoke/                        сквозная проверка Gateway → Kafka → SPPR
internal/app/                     lifecycle и DI service provider
internal/config/env/              загрузка и проверка env
internal/converter/kafka/         интерфейсы decoders; decoder/ — protobuf → domain event
internal/service/consumer/access/ consumer на topic, handlers и классификация ошибок
internal/handler/health/          HTTP health checks
internal/service/ingestor/        validation и транзакция
internal/repository/event/        PostgreSQL queries
internal/model/                   события, ErrRejected и результат обработки
internal/validator/               проверка контракта
internal/observability/           ingestion metrics
pkg/mocks/                        generated minimock mocks
migrations/                       migrate up/down SQL
```

## API

| HTTP | Ответ |
|---|---|
| `GET /healthz` | 200, пока работает HTTP server |
| `GET /readyz` | 200 при доступной PostgreSQL; иначе 503 |

Readiness не отражает Kafka и не говорит о полноте истории: остановленная
partition видна в логах и по метрике `outcome=error`. Admin API и business gRPC
API пока отсутствуют.

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

Параметры sarama — константы `config/env/sarama.go`: `OffsetOldest`,
`Consumer.Return.Errors=true` (ошибки группы читаются и логируются). Отдельного
лимита на попытку нет: `ExecContext` shared PG client ограничен `PG_TIMEOUT`.
Без `PG_DSN` компоненты подключения кодируются как PostgreSQL URL: пробелы и
спецсимволы в пароле не меняют смысл остальных параметров.

Compose использует отдельную development БД на порту 5436, Kafka `kafka:29092`,
HTTP 8086, collector `otel-collector:4317`. Collector запускается отдельно. При
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

## Наблюдаемость и диагностика

| Инструмент | Содержимое |
|---|---|
| `sppr_ingestion_events_total` | попытки с `outcome=stored`, `duplicate`, `rejected`, `error` |
| `sppr_ingestion_missing_events_total` | обнаруженные пропущенные номера |
| `sppr_ingestion_duration_seconds` | длительность попытки |
| structured logs | `Access event rejected`, ошибки ingestion перед retry, ошибки consumer group; topic, partition, offset; `Kafka msg received` на каждое сообщение |

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

Проверяются дубли, конкуренция, пропуски, restart service layer после commit,
атомарный rollback, новый stream после рестарта, отклонения с `ErrRejected`,
decoders и классификация ошибок в handlers. Kafka-тест приложения проверяет
повтор, gap, событие второго topic, ожидание partition при сбое БД и
восстановление, подтверждение недекодируемого события без блокировки
следующего, OTEL metrics/logs и shutdown.

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
экземпляр во внутренней development сети. Прогноз и переносы ещё не выполняются;
производительность и квота 2 ядра / 2 ГиБ пока не подтверждены измерениями.

## Связанные документы

- [Онбординг](ONBOARDING.md) — порядок чтения, алгоритм и ручная проверка.
- [Спека Kafka](../../docs/kafka-spec.md) — контракт, доставка и отказоустойчивость.
- [Gateway](../gateway/README.md) — HTTP PUT/GET.
- [Producer, PR #1](https://github.com/WithSoull/s3-sppr/pull/1) — отдельная публикация обращений.
- [Users](../users/README.md) — образец слоёв и DI.
- [Metadata](../metadata/README.md) — существующие Kafka producers.
- [Правила разработки](../../AGENTS.md).
