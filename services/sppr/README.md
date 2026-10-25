# SPPR Service

Основа сервиса поддержки принятия решений OpenS3 на Go. Текущий срез — Ingestor
из шага 3.1: история обращений, дедупликация, контроль пропусков и агрегаты.

Для первого чтения кода: [онбординг](ONBOARDING.md). Исправления и границы
проверки от 25 октября 2026 года: [ревью](REVIEW.md).

## Назначение и возможности

- принимает версионированные события чтения и записи из Kafka;
- сохраняет историю и координаты доставки в PostgreSQL;
- считает спрос только по успешным клиентским чтениям;
- фиксирует пропущенные номера отдельно от нулевого спроса;
- предоставляет health checks, структурированные логи, OTEL traces и metrics.

Feature Builder, Predictor, Planner и переносы относятся к следующим шагам.
Таблицы моделей, планов и миграций пока не создаются: их схема должна
соответствовать реализованным сценариям.

## Архитектура и зависимости

```text
Kafka → shared/go-kit Consumer → retry middleware → Access handler
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
валидация события и граница транзакции — в service, JSON и trace context — в
handler. Worker управляет Kafka lifecycle.

Используются `shared/pkg/go-kit`: consumer, PostgreSQL client, transaction
manager, logger, metrics и tracing. Sarama создаёт consumer group и позволяет
следить за сессией. Штатный consumer продолжает чтение после ошибки handler;
retry middleware внутри сервиса удерживает сообщение до сохранения или отмены
сессии. Следующий offset не может подтвердиться поверх неуспешного события.

SIGINT/SIGTERM отменяет worker, завершает HTTP-запросы, затем закрывает consumer
group, PostgreSQL и telemetry. Частично созданные зависимости освобождаются при
ошибке startup. Последним закрывается logger с выгрузкой OTLP-логов, если она
включена. Shared closer здесь не используется: его конкурентное закрытие
зависимостей не обеспечивает нужный порядок.

## Структура проекта

```text
cmd/server/                  entrypoint, flags, signals
internal/app/                lifecycle и DI service provider
internal/config/env/         загрузка и проверка env
internal/handler/access/     Kafka payload → domain event
internal/handler/health/     HTTP health checks
internal/service/ingestor/   validation и транзакция
internal/repository/event/  PostgreSQL queries
internal/model/             события и результат обработки
internal/validator/         проверка контракта
internal/worker/ingestion/  consumer lifecycle и retry
internal/observability/     ingestion metrics
pkg/mocks/                  generated minimock mocks
migrations/                 migrate up/down SQL
examples/                   воспроизводимые события
```

## API

| HTTP | Ответ |
|---|---|
| `GET /healthz` | 200, пока работает HTTP server |
| `GET /readyz` | 200 при Kafka session с назначенными partitions, отсутствии заблокированной обработки и доступной PostgreSQL; иначе 503 |

Readiness описывает ingestion, а не пригодность истории для модели. После потери
Kafka readiness меняется при завершении сессии, а не мгновенно. Admin API и
business gRPC API пока отсутствуют.

Topic: `opens3.access.v1`. Общий JSON-контракт —
[`shared/pkg/go-kit/events/access`](../../shared/pkg/go-kit/events/access).
`internal/model/event.go` сохраняет alias для слоёв SPPR.

| Поле | Правило |
|---|---|
| `schema_version` | 1; неизвестные поля и версии отклоняются |
| `gateway_id` | стабильный идентификатор экземпляра Gateway |
| `stream_id` | уникальный идентификатор запуска producer, новый при рестарте |
| `sequence` | int64 от 1, назначается до помещения в очередь |
| `occurred_at` | RFC3339, не убывает внутри потока; UTC, микросекундная точность |
| `bucket`, `object_key` | непустые UTF-8 строки, максимум 255 и 1024 байта |
| `version_id` | обязателен при успехе, максимум 256 байт |
| `operation` | `read` или `write` |
| `origin` | `client` или `migration` |
| `result` | `success` или `error` |
| `bytes` | фактически переданные байты, неотрицательный int64 |

`gateway_id` и `stream_id` соответствуют `[A-Za-z0-9_.-]{1,128}`. Kafka key равен
`gateway_id/stream_id`. Поток принадлежит одному topic/partition. Producer
сохраняет порядок номеров и закрепляет partition на срок потока. Если producer
пересчитывает partition при расширении topic, сначала нужны новые stream IDs.
Для producer рекомендуется idempotence и `acks=all`.
Gateway producer поставляется в независимом [PR #1](https://github.com/WithSoull/s3-sppr/pull/1).
После его подключения outcome PUT/GET публикуется при `ACCESS_EVENTS_ENABLED`.
Версия берётся из Metadata; неуспешный GET не увеличивает спрос даже после
отправленного HTTP200, если передача тела оборвалась. Producer фиксирует partition
при запуске потока, поэтому расширение topic не перемещает текущий stream.
Ingestor можно запускать и тестировать до мержа producer: достаточно Kafka и
PostgreSQL, а события можно отправлять из JSON-примера ниже.

## Данные и основные сценарии

Одна транзакция PostgreSQL выполняет следующие действия:

1. Создаёт и блокирует строку потока `(gateway_id, stream_id)`.
2. Сверяет повтор по идентификатору и SHA-256 канонического события. Идентичный
   повтор ничего не меняет; другой payload под тем же номером — ошибка.
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
закрываются; позднее событие из пропуска блокирует обработку до восстановления.

Повреждённый JSON, нарушение порядка, конфликт идентификатора или сбой БД
блокируют partition с повтором через `KAFKA_RETRY_INTERVAL`. Ошибка видна в логах
и readiness; offset не пропускается. Автоматического DLQ или команды пропуска
пока нет. Перед ручной сменой offsets необходимо сохранить необработанное
событие и отдельно отметить неполноту истории.

## Конфигурация

Окружение имеет приоритет над необязательным `.env`. Флаг `-config-path` задаёт
путь. Полный пример — `.env.example`.

| Переменные | Default / требование |
|---|---|
| `PG_DSN` | предпочтительный способ задать подключение |
| `PG_HOST`, `PG_PORT_INNER`, `PG_DATABASE_NAME`, `PG_USER`, `PG_PASSWORD` | все обязательны без `PG_DSN` |
| `PG_SSL_MODE`, `PG_TIMEOUT`, `PG_LOGGING` | `disable`, `5s`, `false` |
| `KAFKA_BOOTSTRAP` | обязательный список `host:port` через запятую |
| `KAFKA_ACCESS_TOPIC`, `KAFKA_GROUP_ID` | `opens3.access.v1`, `sppr-ingestor-v1` |
| `KAFKA_RETRY_INTERVAL`, `KAFKA_PROCESS_TIMEOUT` | `1s`, `5s`, положительные |
| `KAFKA_MAX_EVENT_BYTES` | 65536, положительное |
| `HTTP_ADDRESS`, `SHUTDOWN_TIMEOUT` | `0.0.0.0:8086`, `15s` |
| `LOGGER_LEVEL`, `LOGGER_AS_JSON`, `LOGGER_ENABLE_OLTP` | задать явно; пример `info`, `true`, `false` |
| `OTEL_SERVICE_NAME`, `OTEL_SERVICE_VERSION`, `OTEL_ENVIRONMENT` | задать явно |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | задать явно, gRPC collector `host:port` |
| `OTEL_METRICS_PUSH_TIMEOUT` | задать явно; интервал экспорта, пример `10s` |

`KAFKA_PROCESS_TIMEOUT` ограничивает одну попытку handler, включая транзакцию.
Сетевые таймауты Kafka используют defaults Sarama и от него не зависят.
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
Gateway и остальные прикладные сервисы не требуются. Topic создаётся broker
автоматически при текущих development настройках; иначе создайте его заранее.

После мержа обоих независимых PR доступен весь контур с реальным Gateway.
Ниже дополнительные команды приёмки; автономный Ingestor от них не зависит:

```bash
make up-sppr-gateway
make smoke-sppr
# Сборка, запуск и проверка одной командой:
make smoke-sppr-local
```

Smoke создаёт уникальный bucket и оставляет его небольшие объекты для диагностики.
Проверяются обычный/пустой/chunked PUT/GET, версия, реальный объём, error event
несуществующего объекта, повтор события без роста спроса и gap в отдельном
потоке `migration`. DSN берётся из `SPPR_SMOKE_PG_DSN`; Makefile по умолчанию
использует development PostgreSQL на 5436. Адреса можно переопределить:

```bash
SPPR_SMOKE_PG_DSN='postgres://user:password@localhost:5436/test_db?sslmode=disable' \
go run ./services/sppr/cmd/smoke -gateway http://127.0.0.1:8080 \
  -metadata 127.0.0.1:50052 -kafka localhost:9092 -topic opens3.access.v1
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

Отправить события из корня репозитория:

```bash
python3 - <<'PY' | docker compose exec -T kafka kafka-console-producer --bootstrap-server kafka:29092 --topic opens3.access.v1 --property parse.key=true --property key.separator='|'
import json
from pathlib import Path
for line in Path('services/sppr/examples/access.jsonl').read_text().splitlines():
    event = json.loads(line)
    print(event['gateway_id'] + '/' + event['stream_id'] + '|' + line)
PY
docker compose exec postgres-sppr psql -U sppr_user -d sppr_db -c 'TABLE read_aggregates; TABLE ingestion_gaps;'
```

В чистой БД: два чтения, 2097152 байта, пропущенный номер 2. Повтор примера не
увеличивает спрос.

## Наблюдаемость и диагностика

| Инструмент | Содержимое |
|---|---|
| `sppr_ingestion_events_total` | попытки с `outcome=stored`, `duplicate`, `error` |
| `sppr_ingestion_missing_events_total` | обнаруженные пропущенные номера |
| `sppr_ingestion_duration_seconds` | длительность попытки |
| span `access.ingest` | Kafka topic/offset, ошибка; W3C context из headers |
| structured logs | consumer/retry errors, topic, partition, offset |

Retries увеличивают число попыток и ошибок, но не число чтений в БД. Object key
и Gateway ID не используются как labels. При 503 проверьте логи partition, БД
и consumer group. Устойчивый сбой JSON требует исправления producer или
восстановления потока; перезапуск не пропускает сообщение.

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
`CREATE SCHEMA`. Полный тест также создаёт/удаляет уникальный Kafka topic и
поднимает локальный gRPC OTLP receiver. Без `SPPR_TEST_KAFKA_BOOTSTRAP`
пропускается только Kafka smoke; PostgreSQL tests требуют `SPPR_TEST_PG_DSN`.
Используйте тестовые зависимости.

Workflow `.github/workflows/sppr-ci.yml` запускает vet и весь набор с `-race`
на PostgreSQL 16 и Kafka 7.7.0 при изменениях сервиса или go-kit.
Дополнительный job сквозного smoke выполняется только при наличии Gateway
producer в checkout. В независимом PR Ingestor он пропускает запуск стенда;
после объединения обоих компонентов выполняет `make smoke-sppr-local`.

Проверяются дубли, конкуренция, пропуски, restart service layer после commit,
атомарный rollback, новый stream после рестарта, повреждённые события, отмена
retry и Kafka offset commit. Kafka smoke проверяет 503 при сбое записи,
восстановление, poison event без пропуска следующего события, OTEL и shutdown.

## Ограничения и дальнейшая работа

Gateway producer поставляется отдельным PR. Полнота истории ещё не определяется: нет
общего watermark и heartbeat. Очередь Gateway находится в памяти; переполнение
тратит sequence и приводит к gap при следующем доставленном событии. Потерянный
хвост при аварии процесса не обнаруживается без дополнительных механизмов.
Таблицы будущих моделей/планов ещё не создаются. На шаге 3.2 нужны
признаки без утечки будущего, часовые метки и правила полноты. Минутного
агрегата недостаточно для произвольной границы T; точные события сохраняются.

Нет retention/partitioning истории, DLQ, восстановления пропусков, SASL/TLS
конфигурации Kafka и административного доступа. Запуск рассчитан на один
экземпляр во внутренней development сети. Прогноз и переносы ещё не выполняются;
производительность и квота 2 ядра / 2 ГиБ пока не подтверждены измерениями.

## Связанные документы

- [Онбординг](ONBOARDING.md) — порядок чтения, алгоритм и ручная проверка.
- [Ревью от 25.10.2026](REVIEW.md) — исправления и выполненные проверки.
- [Gateway](../gateway/README.md) — HTTP PUT/GET.
- [Producer, PR #1](https://github.com/WithSoull/s3-sppr/pull/1) — отдельная публикация обращений.
- [Users](../users/README.md) — образец слоёв и DI.
- [Metadata](../metadata/README.md) — существующие Kafka producers.
- [Правила разработки](../../AGENTS.md).
