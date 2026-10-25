# SPPR: маршрут чтения и первая проверка

Этот текст объясняет текущую реализацию. Команды запуска, полный контракт и
переменные окружения собраны в [README](README.md). Результаты ревью — в
[REVIEW.md](REVIEW.md).

## Что сервис делает сейчас

SPPR пока реализует Ingestor: получает события обращений из Kafka, сохраняет
историю в PostgreSQL и считает минутный спрос на версии объектов. Предсказаний,
выбора HOT/COLD и переносов данных здесь ещё нет.

Gateway уже существует: он обслуживает HTTP PUT/GET через Metadata и Storage.
Producer обращений вынесен в независимый [PR #1](https://github.com/WithSoull/s3-sppr/pull/1).
Он отправляет outcome PUT/GET в `opens3.access.v1` при `ACCESS_EVENTS_ENABLED`.
До его мержа SPPR работает самостоятельно с JSON-примером и integration tests.
События Storage `object-stored` не заменяют историю чтений.

```mermaid
flowchart LR
    Client[HTTP клиент] --> GW[Gateway]
    GW --> Metadata
    GW --> Storage
    GW -. Producer из отдельного PR .-> Producer[Ограниченная очередь обращений]
    Producer --> Kafka
    Kafka --> Worker[Worker + retry]
    Worker --> Handler[Access handler]
    Handler --> Ingestor
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
при каждом новом запуске producer. `sequence` начинается с 1 и растёт внутри
этого потока. Разные потоки не обязаны иметь общее время или общую нумерацию.

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

Прочитай общий [Event](../../shared/pkg/go-kit/events/access/event.go) и
[validator](../../shared/pkg/go-kit/events/access/validate.go).
В SPPR [model/event.go](internal/model/event.go) содержит alias, а
[validator/event.go](internal/validator/event.go) делегирует общую проверку.

`AccessEvent` — содержимое события. `Delivery` — координаты Kafka, полученные
consumer, а не взятые из JSON. `IngestResult` сообщает, был ли повтор и сколько
номеров пропущено перед новым событием.

`IsDemand()` выделяет только `read + client + success`. Запись, неуспешное чтение
и чтение при миграции остаются в истории, но не повышают спрос. Иначе система
начала бы считать собственные переносы пользовательским интересом.

Validator проверяет версию схемы, идентификаторы, допустимые операции,
результаты, время и длины строк. Для успешной операции нужен `version_id`.
Количество байтов может быть нулевым: пустой объект тоже можно успешно прочитать.

### 2. Схема PostgreSQL

Прочитай [up-миграцию](migrations/20261021000000_ingestion.up.sql).

| Таблица | Что нужно увидеть в SQL |
|---|---|
| `ingestion_streams` | одна строка на поток; последний sequence, время и число пропусков |
| `access_events` | уникальность события и координат Kafka; hash полезной нагрузки |
| `ingestion_gaps` | диапазон отсутствующих номеров и временные границы |
| `read_aggregates` | ключ версии объекта и минуты; счётчики чтений и байтов |

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

Здесь проверяются доменное событие и `Delivery`, затем время приводится к UTC
с точностью до микросекунды. Это важно для сравнения с PostgreSQL: разные
представления одного времени не должны давать разные hash или ложную регрессию.

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
4. Посчитать SHA-256 от JSON нормализованной структуры события.
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
нет. Это выбранное ограничение восстановления истории.

Hash считается по полям структуры, поэтому пробелы и порядок полей исходного
JSON не меняют идентичность события. Координаты Kafka в hash не входят:
одинаковое событие может быть повторно опубликовано с другим offset.

Если сломалось обновление агрегата, откатываются также событие, gap и прогресс.
Иначе повторная доставка могла бы увидеть уже сохранённый event и потерять спрос.

### 5. Kafka handler

Прочитай [handler/access/handler.go](internal/handler/access/handler.go).

Handler извлекает W3C trace context из headers, создаёт span, проверяет размер
и UTF-8, декодирует один JSON без неизвестных полей и сверяет Kafka key.
Затем передаёт событие и реальные координаты доставки сервису.

Это транспортная граница: SQL здесь отсутствует. Валидация допустимых операций
остаётся в сервисном слое, чтобы её нельзя было обойти прямым вызовом Ingestor.
Метрики фиксируют результат каждой попытки, включая ошибки и повторы.

### 6. Worker и подтверждение Kafka

Прочитай [worker/ingestion/worker.go](internal/worker/ingestion/worker.go), затем
[shared consumer](../../shared/pkg/go-kit/kafka/consumer/concumer.go) и
[group handler](../../shared/pkg/go-kit/kafka/consumer/group_handler.go).
Имя `concumer.go` — существующее имя файла в общей библиотеке.

Shared consumer после ошибки handler продолжает читать сообщения. Просто вернуть
ошибку из SPPR недостаточно: более позднее успешное сообщение позволило бы
подтвердить offset поверх необработанного события.

Поэтому `retry` находится **внутри вызова handler**. Одна попытка ограничена
`KAFKA_PROCESS_TIMEOUT`. После ошибки worker делает паузу `KAFKA_RETRY_INTERVAL`
и повторяет то же сообщение. Он отдаёт управление shared consumer после успеха
либо отмены сессии. Только после успеха consumer вызывает `MarkMessage`.

`MarkMessage` отмечает offset локально; broker commit выполняет Sarama. Если
процесс упадёт после PostgreSQL commit и до Kafka commit, событие придёт снова,
а repository распознает повтор. Здесь доставка с повторами и однократное
изменение данных в БД благодаря дедупликации.

Ошибка блокирует свою partition, а не все partitions. При этом readiness всего
экземпляра становится false. Повреждённый JSON повторяется так же, как ошибка БД;
автоматического DLQ или пропуска нет. Отмена context завершает ожидание retry.

`observedGroup` и `sessionHandler` оборачивают Sarama lifecycle, чтобы знать,
назначены ли partitions. Это не второй consumer. Сетевые таймауты Sarama
используют её defaults и не зависят от времени обработки одного события.

### 7. Сборка зависимостей и запуск

Теперь прочитай [service_provider.go](internal/app/service_provider.go),
[app.go](internal/app/app.go) и [main.go](cmd/server/main.go).

Provider собирает PG client → repository и transaction manager → Ingestor →
handler; отдельно создаёт Sarama group, затем worker. Зависимости создаются
лениво, но разрешаются на одной goroutine до запуска обработки. Методы provider
не рассчитаны на конкурентную первоначальную сборку.

Перед consumer проверяется наличие таблиц. Это проверка наличия схемы, а не
проверка всех колонок или номеров миграций.

`NewApp` настраивает logger, tracing, metrics, зависимости и HTTP listener.
`Run` запускает HTTP и Kafka worker. Сигнал из main отменяет общий context,
останавливает worker и завершает HTTP. После `Run` вызывается `Close`: HTTP
resources → consumer group → PG pool → metrics → traces → logger.
Ошибки частичного startup тоже приводят к очистке уже созданных ресурсов.

Общий closer в этом приложении не используется: здесь явно задан порядок
закрытия, чтобы БД не закрылась посреди ещё работающего ingestion.

### 8. Конфигурация, health и telemetry

Прочитай [config.go](internal/config/config.go),
[interfaces.go](internal/config/interfaces.go) и файлы
[config/env](internal/config/env). В них только разбор env и проверка настроек:
`pg.go`, `kafka.go`, `http.go`, `logger.go`, `tracing.go`, `metrics.go`.

Настройки окружения важнее `.env`; путь задаётся флагом `-config-path`.
`PG_DSN` важнее отдельных PG-полей. Образ использует `GOWORK=off` и локальный
`replace` на `shared`, поэтому ему не нужны исходники остальных сервисов.

Заверши чтением [health handler](internal/handler/health/handler.go) и
[ingestion metrics](internal/observability/metrics.go).

`/healthz` показывает, что HTTP server отвечает. `/readyz` требует Kafka session
с partitions, отсутствия заблокированной обработки и успешного PG ping. Он не
доказывает отсутствие gaps и готовность истории к обучению модели.

Счётчики telemetry описывают попытки процесса. `read_aggregates` описывает спрос
в зафиксированных данных. Это разные величины: retry увеличивает число ошибок,
но не должен увеличивать чтения. В label метрик не попадают object key и stream
ID, чтобы число временных рядов не росло с каждым объектом.

## Какие тесты читать после реализации

| Файл | Какую гарантию показывает |
|---|---|
| [ingest_test.go](internal/service/ingestor/ingest_test.go) | валидация до БД, нормализация времени, ошибки repository и commit |
| [handle_test.go](internal/handler/access/handle_test.go) | JSON/key, передача события и результата, отказ без вызова Ingestor |
| [retry_test.go](internal/worker/ingestion/retry_test.go) | повтор именно того же сообщения, readiness и отмена ожидания |
| [store_integration_test.go](internal/repository/event/store_integration_test.go) | реальные SQL: дедупликация, gap, спрос, конкуренция и полный rollback |
| [app_integration_test.go](internal/app/app_integration_test.go) | Kafka → PG, блокировка partition, offset, health, OTLP и shutdown |
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

Первое назначение Kafka partitions занимает время. Если readiness ещё 503,
проверь `docker compose logs sppr` и повтори curl после присоединения к группе.
Если другой Compose-проект уже занял 9092 или 5436, сначала проверь
`docker compose ls`: один стенд должен использовать согласованное имя проекта
(`docker compose -p <имя>`). Не запускай второй broker на том же host-порту.

Выполни пример публикации из [README](README.md#примеры-использования).
Он отправляет `1, 1, 3`. Затем выполни:

```bash
docker compose exec postgres-sppr psql -U sppr_user -d sppr_db -c "
SELECT sequence, operation, origin, result, bytes
FROM access_events
WHERE gateway_id='demo-gateway' AND stream_id='demo-run-1'
ORDER BY sequence;
SELECT first_sequence, last_sequence, after_time, before_time
FROM ingestion_gaps
WHERE gateway_id='demo-gateway' AND stream_id='demo-run-1';
SELECT reads, bytes FROM read_aggregates
WHERE bucket='demo' AND object_key='object.bin' AND version_id='v1';"
```

На чистой БД ожидаются события 1 и 3, gap `2..2`, два чтения и 2097152 байта.
Повтори публикацию: числа останутся прежними. Перед изменением JSON выбери новый
`stream_id`, иначе изменённый payload старого события заблокирует partition.

Для проверки отказов используй готовый интеграционный тест. Он создаёт временные
schema и topic и удаляет их после себя:

```bash
SPPR_TEST_PG_DSN='postgres://sppr_user:sppr_password@localhost:5436/sppr_db?sslmode=disable' \
SPPR_TEST_KAFKA_BOOTSTRAP=localhost:9092 \
make test-sppr-integration
go vet ./services/sppr/...
```

Kafka-проверка пропускается без её env. PostgreSQL-проверки требуют явно заданный
DSN. В [CI](../../.github/workflows/sppr-ci.yml) обе зависимости поднимаются
отдельно; там проверяются vet и тесты с `-race`.

## Что учитывать при следующем изменении

Нельзя подтверждать Kafka до commit, обходить `FOR UPDATE` или отдельно
обновлять прогресс и агрегаты. Нельзя считать отсутствие событий нулевым
спросом, пока полнота истории не определена. `last_event_at` одного потока —
не общий watermark W.

Producer Gateway получает version из Metadata и фиксирует outcome после
завершения передачи. Получение reader из Storage ещё не означает успешное
чтение: проверяются EOF и запись последнего байта. На PUT считаются байты,
прочитанные handler из HTTP body, на GET — успешно записанные HTTP writer.
Эти значения не доказывают чтение данных клиентским приложением.

После мержа producer прочитай `services/gateway/internal/publisher/access/publisher.go`,
`services/gateway/internal/app/access.go`, затем изменения GET/PUT handler.
Номер, время и enqueue упорядочены под mutex; отправитель повторяет одно событие,
не давая следующим его обогнать. Переполнение не заставляет HTTP ждать:
событие отбрасывается с потраченным номером. Очередь в памяти теряется при аварии.
Гарантии и критерии проверки собраны в
[PR producer](https://github.com/WithSoull/s3-sppr/pull/1).

После мержа обоих PR для проверки всего пути используй `make smoke-sppr-local` из корня.
Он поднимает Gateway/SPPR и проверяет HTTP → Kafka → PostgreSQL, включая replay
и отдельный поток с gap. Команды автономного примера выше остаются полезны
для проверки Ingestor без остальных сервисов.

Нет восстановления gaps, контроля потерянного хвоста, retention, DLQ и
административной команды пропуска. История пока растёт без ограничения. Это
границы текущего Ingestor, которые нужно учесть до Feature Builder и эксперимента.
