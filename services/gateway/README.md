# Gateway Service

Минимальный HTTP-вход в локальный стенд OpenS3-ReBAC: PUT/GET/DELETE объекта с
классами хранения HOT/COLD, а не полный S3 API.

## Назначение и возможности

Gateway принимает PUT, GET (целиком или один диапазон Range) и DELETE объекта,
передаёт байты в Storage того класса, который назначил Metadata, и работает с
каталогом Metadata. HTTP-сервер также предоставляет `GET /healthz` и завершает
активные запросы при SIGINT/SIGTERM. Для локальной разработки доступны Swagger
UI, нормализованные HTTP-метрики и трассировка без bucket/key в labels.
При `ACCESS_EVENTS_ENABLED=true` публикует успешные GET и PUT в Kafka для SPPR.

## Архитектура и зависимости

```text
HTTP client → Gateway → Metadata gRPC (bucket, версии, класс записи и расположение)
                      → Storage HOT gRPC  ┐ байты blob; экземпляр выбирается
                      → Storage COLD gRPC ┘ по классу версии
     object service → AccessProducerService → sarama AsyncProducer → Kafka object-read
                                                                    → Kafka object-written → SPPR
```

Для работы с объектами нужны доступные Metadata и два экземпляра Storage: HOT и
COLD на разных томах. Адреса экземпляров Gateway берёт из своей конфигурации:
в `storage_classes` Metadata endpoint не хранится. При включённых
событиях Gateway также подключается к Kafka; PostgreSQL остаётся зависимостью
Metadata и SPPR. Внутренние gRPC-соединения в локальном стенде используют plaintext.
Контракт — `shared/api/events/v1/access.proto`, решения — [спека Kafka](../../docs/kafka-spec.md).
Как в UserServer из `messenger-overview` (раздел 5.1 спеки), на процесс создаётся
один `sarama.AsyncProducer` и по `kafka.Producer` из go-kit на topic. HTTP handlers про события не
знают. Consumer SPPR поставляется отдельным PR; Gateway запускается и публикует
события без него.

## Структура проекта

```text
cmd/server/                 запуск процесса, выбор файла настроек и обработка сигналов
cmd/smoke/                  проверка живого HTTP → Metadata/Storage контура
internal/config/env/        загрузка настроек по компонентам из окружения
internal/app/               загрузка настроек, сборка зависимостей и жизненный цикл HTTP-сервера
internal/client/            адаптеры Metadata и Storage, streaming и trace propagation
internal/model/             модели объекта и результаты PUT/GET
internal/errors/domain_errors/ ошибки сценариев и чтения тела запроса
internal/service/object/    сценарии PUT/GET/DELETE с Metadata и Storage нужного класса
internal/handler/httpapi/   HTTP-маршруты, заголовки и коды ответов
internal/observability/     HTTP spans и метрики с нормализованными route
internal/service/producer/access/ потоки с номерами и публикация ObjectRead/ObjectWritten; noop при выключенных событиях
pkg/mocks/                  сгенерированные minimock для клиентов, сервисов и kafka.Producer
docs/                       сгенерированная Swagger/OpenAPI 2.0 схема
```

В `internal/client/storage/` загрузка и чтение разделены на `store.go` и
`retrieve.go`; управление span и передача trace context находятся в `tracing.go`.
В `internal/service/object/` сценарии вынесены в `put.go`, `get.go` и
`delete.go`, а `service.go` содержит зависимости, конструктор и выбор клиента
Storage по классу (`map[model.StorageClass]client.StorageClient`).
Контракты Metadata/Storage находятся в `internal/client/client.go`, отдельно
от gRPC-реализаций. Результат GET передаёт вызывающему коду `io.ReadCloser`;
получатель обязан закрыть поток, в том числе при ошибке передачи ответа.

## API

| Метод и путь | Результат |
|---|---|
| `PUT /objects/{bucket}/{key}` | сохранить тело и зарегистрировать текущую версию; `200`, `ETag`, `X-Object-Version-Id` |
| `GET /objects/{bucket}/{key}` | вернуть байты текущей версии, `Content-Type`, `Content-Length`, `ETag`, `Accept-Ranges: bytes` |
| `GET` с `Range: bytes=…` | один диапазон: `206` и `Content-Range`; диапазон за концом объекта — `416` и `Content-Range: bytes */size` |
| `DELETE /objects/{bucket}/{key}` | удалить объект из каталога Metadata; `204`, отсутствующий объект — `404` |
| `GET /healthz` | `200 ok` при работающем HTTP-процессе |
| `GET /swagger/index.html` | Swagger UI локального development API |
| `GET /swagger/doc.json` | Swagger/OpenAPI 2.0 схема |

`key` может содержать `/`. Пустые bucket/key не принимаются. GET по `version_id`,
HEAD, multipart и S3 authentication пока отсутствуют. Range поддерживает
`bytes=first-last`, `bytes=first-` и `bytes=-suffix`; несколько диапазонов,
другая единица или неверный синтаксис игнорируются, и Gateway отдаёт объект
целиком (RFC 9110 это допускает). DELETE в отличие от S3 возвращает `404` для
отсутствующего объекта. Байты удалённого объекта остаются в Storage: очистки blob
пока нет (consumer `object-deleted` в Storage не подключён).
Объекты доступны только под `/objects/`; прежний путь `/{bucket}/{key}` больше
не поддерживается. `/swagger/` и `/healthz` относятся к процессу Gateway.
Bucket `swagger` доступен как `/objects/swagger/{key}`. Health
проверяет сам процесс, а не готовность Metadata и Storage.

## Данные и основные сценарии

PUT проверяет существование bucket через Metadata `HeadBucket` и спрашивает
`ChooseWriteClass(bucket, key, size)`. Metadata отвечает HOT, только если текущая
версия ключа лежит в HOT, размер известен (есть `Content-Length`) и в HOT хватает
места; иначе COLD. Новый ключ всегда пишется в COLD. Затем Gateway отправляет
тело потоком в `StoreObject` Storage этого класса и вызывает `CreateObjectVersion`
с полученным `blob_id`, размером, MD5 и тем же классом. HTTP-успех возвращается
после ответа Metadata. Регистрация версии после записи blob не отменяется
разрывом HTTP-соединения; её ограничивает только `METADATA_RPC_TIMEOUT`.

Место в HOT при выборе класса не резервируется: одновременные перезаписи HOT-ключей
могут превысить `usable_bytes`. Перебор виден в Metadata `ListStorageClasses` как
отрицательное `free_bytes`; запас между ёмкостью и `max_utilization` его
поглощает, а исправляет Planner выселением. Прежняя версия перезаписанного
HOT-ключа остаётся в HOT как нетекущая, пока её не выселит Planner.

GET получает через `GetObjectMeta` `blob_id` и класс действующего расположения
версии и передаёт ответ `RetrieveObject` Storage этого класса клиенту потоком.
Неизвестный класс даёт `502`. Целый объект запрашивается у Storage целиком, а не
`Content-Length` байт, поэтому blob длиннее записанного в Metadata обнаруживается.
Для диапазона Gateway сначала получает размер из Metadata, затем запрашивает у
Storage только `offset/length`. Последний байт удерживается до
успешного завершения потока Storage; при ошибке или неверном размере HTTP-ответ
обрывается до передачи полного Content-Length. Для пустого объекта EOF
проверяется до отправки успешного ответа.

Этот путь использует существующий `CreateObjectVersion`, который сразу создаёт
committed-версию. `pending/committed/aborted` orchestration, operation ID и
восстановление после разрыва между записью blob и Metadata ещё не реализованы.
При ошибке Metadata после успешной записи в Storage может остаться blob без
ссылки в каталоге.

Если Metadata успела зарегистрировать версию, но её ответ потерян, PUT тоже
завершится ошибкой, хотя новая версия уже доступна. Поэтому ошибка PUT после
записи blob не определяет итог операции. Автоматический повтор может создать
ещё одну версию: идемпотентность и проверка результата по operation ID пока
отсутствуют.

При включённых событиях object service публикует только успешные операции.
`ObjectRead` уходит в `object-read` после открытия потока из Storage; `bytes` —
размер объекта или длина отданного диапазона. Чтение, оборванное на середине передачи,
считается чтением: спрос был. `ObjectWritten` уходит в `object-written` после
`CreateObjectVersion`, даже если клиент уже отменил запрос; `bytes` — размер,
сохранённый Storage. Ошибки, health/swagger и неверные пути событий не создают.
`origin` пока всегда `ORIGIN_CLIENT`; признак Migration Executor появится в шаге 3.6.

У каждого topic свой поток: `stream_id` — новый UUID при старте, `sequence` — с
1 без дыр. Kafka key — `gateway_id/stream_id`, поэтому поток попадает в одну
partition. Присвоение номера и времени, marshal и `Send` выполняются под одним
мьютексом потока, иначе событие N+1 могло бы попасть в sarama раньше N.

`Send` не ждёт Kafka: сообщение передаётся в `Input()` sarama, буфер, ретраи и
порядок дальше обеспечивает sarama. `Input()` небуферизованный, поэтому `Send`
ждёт передачи около 1 мс (таймер); если буферы sarama полны, возвращается
`ErrBufferFull`. Номер при этом потрачен, и SPPR увидит пропуск. Окончательная
ошибка доставки после ретраев приходит в `Errors()` и тоже становится пропуском.
Методы `AccessProducerService` ошибку не возвращают: событие — телеметрия, и
S3-операция её не видит.

## Конфигурация

| Переменная | Локальное значение по умолчанию | Compose |
|---|---|---|
| `GATEWAY_HTTP_ADDR` | `127.0.0.1:8080` | `:8080` |
| `METADATA_GRPC_ADDR` | `localhost:50052` | `metadata:50052` |
| `METADATA_RPC_TIMEOUT` | `5s` | `${METADATA_RPC_TIMEOUT:-5s}` |
| `STORAGE_HOT_GRPC_ADDR` | `localhost:50056` | `storage-hot:50053` |
| `STORAGE_COLD_GRPC_ADDR` | `localhost:50053` | `storage-cold:50053` |
| `GATEWAY_MAX_READ_DURATION` | `5m` | `${GATEWAY_MAX_READ_DURATION:-5m}` |
| `STORAGE_RETRIEVE_CHUNK_SIZE_BYTES` | `1048576` (1 MiB) | `${STORAGE_RETRIEVE_CHUNK_SIZE_BYTES:-1048576}` |
| `ACCESS_EVENTS_ENABLED` | `false` | `${ACCESS_EVENTS_ENABLED:-true}` |
| `GATEWAY_ID` | обязателен при включении; SPPR принимает `[A-Za-z0-9_.-]{1,128}` | `${GATEWAY_ID:-gateway-1}` |
| `KAFKA_BOOTSTRAP` | `localhost:9092`; список `host:port` через запятую | `kafka:29092` |
| `KAFKA_OBJECT_READ_TOPIC` | `object-read` | `${KAFKA_OBJECT_READ_TOPIC:-object-read}` |
| `KAFKA_OBJECT_WRITTEN_TOPIC` | `object-written` | `${KAFKA_OBJECT_WRITTEN_TOPIC:-object-written}` |
| `LOGGER_LEVEL` | `info` | `info` |
| `LOGGER_AS_JSON` | `false` | `false` |
| `LOGGER_ENABLE_OLTP` | `false` | `false` |
| `OTEL_ENABLED` | `false` | `${GATEWAY_OTEL_ENABLED:-false}` |
| `OTEL_SERVICE_NAME` | `gateway` | `gateway` |
| `OTEL_SERVICE_VERSION` | `dev` | `${GATEWAY_SERVICE_VERSION:-dev}` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | `${GATEWAY_OTLP_ENDPOINT:-otel-collector:4317}` |
| `OTEL_ENVIRONMENT` | `local` | `${GATEWAY_ENVIRONMENT:-local}` |
| `OTEL_METRICS_PUSH_TIMEOUT` | `30s` | `${GATEWAY_METRICS_PUSH_TIMEOUT:-30s}` |

При `ACCESS_EVENTS_ENABLED=false` provider отдаёт noop-сервис, Kafka client не
создаётся. При включении Kafka должна быть доступна на startup: иначе
`sarama.NewAsyncProducer` возвращает ошибку и Gateway не стартует. Отказ Kafka
после старта обрабатывают буфер и ретраи sarama.

Параметры sarama — константы `internal/config/env/sarama.go`, не env:
idempotent producer, `RequiredAcks=WaitForAll`, `Net.MaxOpenRequests=1`,
`Return.Successes=false`, `ChannelBufferSize=1024`, `Retry.Max=5`,
`Retry.Backoff=200ms`, `Version=V2_8_0_0`. После HTTP drain closer вызывает
`AsyncProducer.Close()`: sarama дотправляет буфер в пределах общего shutdown
timeout (10 s). Пример локальных настроек — [.env.example](.env.example).

Каждый Metadata RPC ограничен `METADATA_RPC_TIMEOUT` (положительная duration).
Более короткий deadline исходного контекста сохраняется. Таймаут действует
отдельно на HeadBucket, ChooseWriteClass, CreateObjectVersion, GetObjectMeta и
DeleteObjectMeta, не на передачу байтов Storage; зависший Metadata возвращает
HTTP 504.

`GATEWAY_MAX_READ_DURATION` (положительная duration) ограничивает весь GET: от
запроса в Metadata до последнего байта из Storage. Если чтение не уложилось,
ответ обрывается так же, как при ошибке Storage. Это граница для переноса между
классами: исполнитель удаляет прежний blob не раньше, чем через `cleanup_after`
после `SwitchPlacement`, поэтому задержку очистки нужно задавать больше этого
значения. Иначе медленное чтение, начатое до переключения, может не найти файл.
Медленному клиенту при большом объекте может понадобиться увеличить значение. Таймаут регистрации
после записи blob по-прежнему может означать неизвестный итог PUT.

Gateway читает переменные окружения и при наличии файл `.env` из текущего
каталога. Другой файл можно указать через `-config-path`. Как и в Users,
логирование настроено через общий модуль платформы. `LOGGER_ENABLE_OLTP`
управляет экспортом логов, а `OTEL_ENABLED` — экспортом метрик и трейсов; оба по
умолчанию выключены.

Logger, tracing и metrics загружаются отдельными конфигурациями. Они используют
те же общие `OTEL_*` значения для имени сервиса и адреса коллектора;
`OTEL_ENABLED` по-прежнему одновременно включает tracing и metrics.

Compose публикует HTTP-порт только на `127.0.0.1:8080`, поскольку в этом срезе
пока нет клиентской аутентификации и авторизации.

Размер чанка чтения настраивает Storage, а Gateway использует то же значение для
своего gRPC receive limit с небольшим запасом под protobuf envelope. В Compose
это один общий параметр окружения; при отдельном запуске процессов одинаковое
значение нужно передать обоим сервисам.

## Запуск

Из корня репозитория:

```bash
make up-gateway
docker compose --profile services ps gateway metadata storage-hot storage-cold
```

Команда поднимает Gateway, Metadata, `storage-hot` (том `storage-hot-data`, порт
хоста 50056) и `storage-cold` (том `storage-data`, порт 50053) и инфраструктурные
зависимости Metadata. COLD остался на прежнем томе, потому что миграция Metadata
отнесла все существующие версии к COLD. Kafka запускается после проверки готовности ZooKeeper. Bucket должен быть создан заранее через Metadata. Для запуска вне
Docker поднимите Metadata и оба Storage (и Kafka, если события включены), затем:

```bash
cd services/gateway
go run ./cmd/server
```

## Примеры использования

Создайте тестовый bucket через существующий Metadata RPC (нужен `grpcurl`):

```bash
grpcurl -plaintext -d '{"name":"gateway-demo","owner_id":"11111111-1111-4111-8111-111111111111"}' \
  localhost:50052 opens3.metadata.v1.MetadataService/CreateBucket
```

После этого:

```bash
printf 'hello' | curl -i -X PUT -H 'Content-Type: text/plain' --data-binary @- \
  http://127.0.0.1:8080/objects/gateway-demo/example.txt
curl -i http://127.0.0.1:8080/objects/gateway-demo/example.txt
curl -i -H 'Range: bytes=1-3' http://127.0.0.1:8080/objects/gateway-demo/example.txt  # 206, "ell"
grpcurl -plaintext -d '{"bucket_name":"gateway-demo","key":"example.txt"}' \
  localhost:50052 opens3.metadata.v1.MetadataService/GetObjectMeta  # storage_class: COLD
curl -i -X DELETE http://127.0.0.1:8080/objects/gateway-demo/example.txt  # 204
curl -i http://127.0.0.1:8080/healthz
# Swagger UI: http://127.0.0.1:8080/swagger/index.html
```

## Наблюдаемость и диагностика

Ошибки downstream gRPC отображаются в HTTP 400/404/503/504/507 или 502:
нехватка места в Storage при записи — 507, перегрузка Metadata — 503,
отсутствие blob в Storage при существующей версии в Metadata — 502, класс, для
которого у Gateway нет Storage, — 502, диапазон за концом объекта — 416. При
ошибке регистрации уже записанного blob Gateway пишет `blob_id` в лог для
ручной диагностики. Метрики `http_server_requests_total` и
`http_server_request_duration_seconds` содержат HTTP method, нормализованный
route и status. HTTP trace context передаётся в Metadata и streaming-вызовы
Storage. Для логов контейнера: `docker compose logs -f gateway`.

Потерянные события видны в двух счётчиках. `gateway_access_events_dropped_total`
(`event=object_read|object_written`, `reason=buffer_full|error`) считает события,
которые sarama не принял. `gateway_kafka_delivery_errors_total` (`topic`) считает
сообщения, которые sarama не доставил после ретраев. Каждая потеря пишется в лог
(`Access event dropped`, `Failed to deliver message`); object keys и stream IDs в
labels отсутствуют. `/healthz` не проверяет Kafka: полноту истории показывает
`ingestion_gaps` в SPPR.

## Разработка и тесты

```bash
make test-gateway
(cd services/gateway && go generate ./pkg/mocks)  # после изменения интерфейсов
make -C services/gateway swagger  # после изменения HTTP-аннотаций
make smoke-gateway-local  # собрать/поднять стенд и проверить PUT/GET
# Для уже запущенного стенда: make smoke-gateway
docker compose --profile services config --quiet
```

Smoke сам создаёт уникальный bucket `gateway-smoke-<UUID>` и проверяет пустой
объект, бинарные данные больше 20 MiB, chunked PUT без Content-Length и перезапись
ключа. Каждый GET выполняется сразу после PUT, без повторов; проверяются SHA-256,
MD5/ETag, размер, Content-Type и версия.

Затем smoke проверяет классы хранения (`cmd/smoke/classes.go`): новый ключ
попадает в COLD; версия вручную переносится в HOT по протоколу `PlacementService`
(ReserveCopy → чтение из COLD → запись в HOT с выданным `blob_id` → сверка MD5 →
SwitchPlacement) — исполнителя миграций пока нет, smoke делает его шаги сам. После
переноса GET возвращает те же байты; blob с новым ID есть только в HOT, так что
это доказывает чтение из HOT. Дальше проверяются Range (206 через границу чанка
Storage, suffix, 416), перезапись HOT-ключа с размером (остаётся в HOT), chunked
перезапись (уходит в COLD) и DELETE (204, затем 404). Прежний COLD blob остаётся
в состоянии `cleanup_pending`: удаляет его исполнитель, которого ещё нет.

Данные остаются в тестовом bucket для диагностики (около 24 MiB на запуск). Нужны
Go 1.24.1 и Docker Compose; grpcurl для smoke не нужен. Общий таймаут smoke —
2 минуты, включая ожидание готовности. Адреса и таймаут можно задать флагами:

```bash
go run ./services/gateway/cmd/smoke -gateway http://127.0.0.1:8080 -metadata 127.0.0.1:50052 \
  -storage-hot 127.0.0.1:50056 -storage-cold 127.0.0.1:50053 -timeout 3m
```

Тесты разложены по слоям, и каждый факт проверяется в одном месте:

- `internal/client/{metadata,storage}`: gRPC-код → доменная ошибка (`ErrMetadataOverloaded`,
  `ErrInsufficientStorage`, `ErrBlobMissing`), ошибка чтения тела, пустой поток;
  `internal/client/propagation_test.go`: deadline вызывающего доходит до Metadata и Storage.
- `internal/handler/httpapi/errors_test.go`: единственная таблица «ошибка → HTTP-статус».
  Unit-тесты handler (`*_unit_test.go`, minimock) проверяют разбор пути, аргументы вызова
  сервиса, заголовки и закрытие тела (включая обрыв GET), а не повторяют таблицу статусов.
  Каждый case создаёт свой minimock controller; версия генератора minimock закреплена в
  `pkg/mocks/generate.go`.
- `internal/service/object/tests/`: порядок вызовов Metadata и Storage, выбор Storage
  по классу при PUT и GET, передача класса в `CreateVersion`, разрешение Range и 416
  без обращения к Storage, `WithoutCancel` при регистрации версии, отсутствие
  downstream-вызовов при раннем отказе, вызов `AccessProducerService` только после успеха.
- `internal/handler/httpapi/*_test.go` (component): настоящий HTTP и настоящие gRPC-клиенты
  поверх фейков Metadata/Storage. Проверяют потоковую передачу PUT до EOF, публикацию версии
  только после commit Storage, обрыв GET, отмену клиента и то, что неудачный PUT ничего не
  оставляет (`TestUpstreamFailures`). `storage_class_test.go`: запись и чтение идут в
  Storage нужного класса, `ChooseWriteClass` получает размер только при
  `Content-Length`. `range_test.go`: разбор Range и заголовки 206/416.
  `delete_test.go`: 204/404 и сохранение blob. `read_duration_test.go`: GET обрывается
  по `GATEWAY_MAX_READ_DURATION`, и тот же срок получает Metadata.
- `internal/app`: сборка приложения (`newTestApp`), 503 при недоступном Metadata,
  `METADATA_RPC_TIMEOUT` → 504, graceful shutdown, noop без Kafka client при
  выключенных событиях, ошибка startup при недоступной Kafka.
- `internal/config/env/config_test.go`: имена переменных, валидация и корректность
  idempotent-настроек sarama.
- `internal/service/producer/access/`: номера идут подряд, `ErrBufferFull` тратит
  номер, у каждого topic свой `stream_id` и счётчик, конкурентные события попадают
  в producer по порядку номеров (проверяется под `-race`).
- `shared/pkg/go-kit/kafka/producer/async_test.go`: `Send` при полном буфере
  возвращает `ErrBufferFull` без ожидания Kafka; `LogErrors` вычитывает ошибки.

Фейки Metadata и Storage общие и лежат в `internal/testkit` (`testkit.New()`, затем
`Dial` для bufconn или `ListenTCP` для настоящего порта; оба обслуживают Metadata и
COLD Storage, `DialStorage`/`ListenStorageTCP` — Storage другого класса). Фейк
Storage каждого класса видит только свои blobs (`BlobClass`), поэтому чтение не
из того экземпляра падает, а не проходит случайно. `Seed` кладёт объект в COLD,
`SeedIn` — в заданный класс; ответ `ChooseWriteClass` задаёт `WriteClass`, а
переданные размеры видны в `WriteSizes`. Поведение настраивается полями и
хуками: `FailStore`, `FailRetrieve`, `FailMetadata(err, ops...)`, `OnMetadata`,
`BeforeCommit`, `BeforeNextChunk`. Новый сценарий добавляйте строкой в существующую
таблицу, а не новым фейком. Всё это дополняет smoke с настоящими сервисами.

## Ограничения и дальнейшие работы

Этот локальный HTTP endpoint пока не проверяет пользователя и права, не
резервирует Quota, не поддерживает S3 SigV4 и не обещает атомарность Storage с
Metadata. DELETE не удаляет blobs; несколько диапазонов в одном Range
(`multipart/byteranges`) не поддерживаются. Исполнителя переносов между
классами пока нет: перенос выполняется вручную через `PlacementService`, как в smoke. Следующий шаг для жизненного цикла версии: `operation_id`, создание
pending-версии и отдельная фиксация в Metadata. После этого можно добавить
восстановление зависших операций.

События обращений не сохраняются на диск в Gateway: crash может потерять хвост
буфера sarama. Без следующего события такой хвост не обнаруживается; это закроет
heartbeat. Durable outbox, heartbeat, trace context в Kafka, общий watermark и
восстановление gaps в этот срез не входят.

## Связанные документы

- [Metadata](../metadata/README.md) и [Storage](../storage/README.md).
- [Спека событий доступа через Kafka](../../docs/kafka-spec.md).
- [Проект политики Gateway](../../docs/gateway-contract-plan.md).
- [Первый запуск](../../GETTING_STARTED.md).
