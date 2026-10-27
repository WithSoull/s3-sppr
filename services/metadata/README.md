# Metadata Service

Go 1.24.1 gRPC-сервис каталога объектного хранилища: buckets, object keys, versions,
расположение версий в классах хранения HOT/COLD и ссылки на blobs в PostgreSQL.
Порт локального стенда — `50052`.

## Назначение и возможности

- создание, получение, перечисление и удаление buckets;
- регистрация committed blob-версии и переключение current version;
- получение текущей или конкретной committed версии;
- постраничный список объектов с фильтром prefix;
- удаление метаданных и публикация событий удаления; blobs всех версий
  удалённого объекта остаются в учёте занятости до очистки;
- действующее расположение каждой committed blob-версии: класс HOT/COLD, blob_id
  и поколение записи; выбор класса новой версии (`ChooseWriteClass`): новый ключ —
  COLD, перезапись HOT-ключа остаётся в HOT, если размер известен и есть место;
- API безопасного переноса версии между классами для исполнителя миграций:
  резервирование места под копию, переключение расположения CAS, отмена и учёт
  очистки старых blobs (`PlacementService`);
- занятость классов с учётом временных копий и каталог для Planner: текущие версии
  и, по запросу, нетекущие версии в HOT;
- custom проверка PostgreSQL/Kafka, standard gRPC health, metrics и tracing.

Сервис не хранит байты, не проверяет JWT/ReBAC и не вызывает Storage. Переданный
`blob_id` связывается с версией без проверки наличия файла на Storage. Копирование
и сверку MD5 при переносе выполняет исполнитель миграций; Metadata хранит только
состояние и переключает расположение.

## Архитектура и зависимости

```text
gRPC MetadataService  → handler/metadata
                          ├── service/bucket → repository/bucket → PostgreSQL
                          └── service/object → repository/object,
                                   │           repository/placement → PostgreSQL
                                   └── Kafka producer (delete events)
gRPC PlacementService → handler/placement
                          └── service/placement → repository/placement,
                                                  repository/placementcopy → PostgreSQL
```

Оба gRPC-сервиса работают в одном процессе на одном порту.

Service задаёт транзакционные границы, repository выполняет SQL, handler переводит
запросы/ответы и ошибки. Общие DB client, transaction manager, Kafka producer и
observability находятся в `shared/pkg/go-kit`.

| Зависимость | В Compose | С хоста | Назначение |
|---|---|---|---|
| PostgreSQL Metadata | `postgres-metadata:5432` | `localhost:5433` | каталог |
| Kafka | `kafka:29092` | `localhost:9092` | события удаления |
| OTEL Collector | `otel-collector:4317` | `localhost:4317` | экспорт telemetry |

Users использует другую PostgreSQL. Между БД Users и Metadata нет общего FK или
транзакции: `owner_id` хранится как UUID.

## Структура проекта

```text
cmd/server/                        точка входа
internal/app/                      wiring и lifecycle
internal/config/                   env configuration
internal/handler/metadata/         transport MetadataService
internal/handler/placement/        transport PlacementService
internal/service/bucket/           bucket operations
internal/service/object/           version/catalog operations
internal/service/placement/        протокол переноса и каталог расположений
internal/repository/bucket/        SQL buckets
internal/repository/object/        SQL objects/versions
internal/repository/placement/     SQL version_placements, занятость классов
internal/repository/placementcopy/ SQL placement_copies
internal/model/                    domain model
pkg/mocks/                         minimock dependencies
migrations/                        PostgreSQL schema (goose)
```

## API

Wire-контракты: [metadata.proto](../../shared/api/metadata/v1/metadata.proto),
`opens3.metadata.v1.MetadataService`, и
[placement.proto](../../shared/api/metadata/v1/placement.proto),
`opens3.metadata.v1.PlacementService`. Комментарии proto о будущем Gateway/cleanup
не подтверждают наличие этих consumers; текущее поведение описано ниже.

| RPC | Вход / результат |
|---|---|
| `CreateBucket` | name + owner_id → bucket_id + created_at |
| `GetBucket` | bucket_name → BucketInfo |
| `HeadBucket` | bucket_name → exists, bucket_id, owner_id |
| `ListBuckets` | owner_id → список принадлежащих ему buckets |
| `DeleteBucket` | bucket_name → success после проверки числа objects |
| `ChooseWriteClass` | bucket_name, key, необязательный size_bytes → storage_class (`HOT`/`COLD`), reason |
| `CreateObjectVersion` | bucket_name, key, blob_id, size_bytes, etag, content_type, storage_class (пусто — COLD) → object_id, version_id |
| `GetObjectMeta` | bucket_name, key, необязательный version_id → blob_id, storage_class, placement_generation и metadata |
| `ListObjects` | bucket_name, prefix, continuation_token, max_keys → объекты и следующий token |
| `DeleteObjectMeta` | bucket_name + key → object_id, текущий blob_id, success; blobs всех версий становятся копиями `orphan` для очистки |
| `HealthCheck` | пустой запрос → SERVING / NOT_SERVING по PostgreSQL и Kafka |

`ChooseWriteClass` вызывается до записи байтов, потому что Gateway должен знать,
в какой экземпляр Storage писать. Ответ HOT (`reason=inherited`) только если
текущая версия ключа лежит в HOT, размер передан и `free_bytes` HOT не меньше
него. Иначе COLD с причиной: `new_key`, `previous_cold`, `size_unknown`
(клиент не передал Content-Length) или `hot_full`. Место не резервируется:
одновременные перезаписи могут превысить `usable_bytes`, и `free_bytes` станет
отрицательным; это поглощает запас `max_utilization`, а исправляет Planner.
Решение «перезапись HOT-ключа остаётся в HOT» опирается на то, что администратор
уже одобрил HOT для этого ключа. Без него новая версия часто читаемого ключа
читалась бы из COLD до следующего плана. Прежняя версия остаётся в HOT как
нетекущая, пока её не выселит Planner.

`CreateObjectVersion` сохраняет класс, переданный Gateway, и ёмкость повторно не
проверяет: blob к этому моменту уже записан в Storage этого класса.

`GetObjectMeta` и `DeleteObjectMeta` берут `blob_id` из действующего расположения
(`version_placements`), а не из `versions.blob_id`: после переноса версия читается
из новой копии.

`PlacementService` (клиенты — СППР и исполнитель миграций):

| RPC | Вход / результат |
|---|---|
| `ListStorageClasses` | → ёмкость, допустимая доля, занятость расположениями и копиями, свободное место |
| `ListPlacements` | page_size, page_token, include_noncurrent_hot → каталог для Planner: текущие версии активных объектов и, с флагом, их нетекущие версии в HOT |
| `GetPlacement` | version_id → расположение и признак текущей версии |
| `ReserveCopy` | migration_id, version_id, expected_generation, source_class, target_class → целевая копия с новым blob_id |
| `SwitchPlacement` | migration_id, проверенные размер и MD5, пауза до удаления источника → новое поколение и копия-источник |
| `AbortCopy` | migration_id, reason → целевая копия в cleanup_pending |
| `ListMigrationCopies` | migration_id → копии миграции для восстановления после перезапуска |
| `ListDueCleanups` | limit → копии, blob которых пора удалить: отменённые цели, источники переносов и blobs удалённых объектов |
| `CompleteCleanup` | copy_id, deleted, error_message → deleted или cleanup_failed |

`ListPlacements` без `include_noncurrent_hot` (или с `false`) возвращает только
текущие версии активных объектов. При `true` дополнительно возвращает нетекущие
версии этих объектов, чьё действующее расположение — HOT (`is_current=false`).
Удалённые и другие неактивные объекты не попадают в каталог. Общий порядок —
по `version_id`, без отдельной группы для нетекущих версий. `page_token` — UUID
последней строки предыдущей страницы; пустая строка означает первую страницу,
невалидный UUID даёт `INVALID_ARGUMENT`. `page_size <= 0` или `> 1000`
нормализуется к 1000. Непустой `next_page_token` возвращается только при наличии
следующей страницы; при обходе сохраняйте значение флага. Каждая строка содержит
`version_created_at` — `versions.created_at` в Unix ms, в отличие от `updated_at`
расположения. Это поле также заполняется в `GetPlacement`.

Повтор `ReserveCopy` и `SwitchPlacement` с тем же migration_id возвращает уже
сохранённый результат и не начинает вторую миграцию. Коды отказов: `ABORTED`
(объект не активен, нетекущая версия переносится не в COLD, расположение
изменилось или у версии уже есть зарезервированная копия), `RESOURCE_EXHAUSTED`
(в целевом классе нет места), `FAILED_PRECONDITION`
(недопустимое состояние копии, несовпадение размера, повторное использование
migration_id с другими параметрами).

`last_modified` сейчас берётся из `versions.created_at`. ListObjects выбирает
active objects с current committed blob version, сортирует по key; token — последний
key предыдущей страницы. `max_keys <= 0` или `> 1000` нормализуется к 1000.
Если строк ровно `max_keys`, `is_truncated=true` даже без следующей строки —
последующий запрос может вернуть пустую страницу. Prefix использует SQL LIKE,
поэтому `%`/`_` пока имеют смысл SQL wildcard, а не буквальных символов S3 prefix.

## Данные и основные сценарии

### Схема каталога

```text
buckets (id, name UNIQUE, owner_id)
  └── objects (id, bucket_id, key, status,
               current_version_id, pending_version_id; UNIQUE(bucket_id, key))
        └── versions (id, object_id, version_number, kind, state,
                      blob_id, size_bytes, etag, content_type,
                      created_at, committed_at, aborted_at)
              ├── version_placements (version_id PK, storage_class, blob_id,
              │                       generation, updated_at)
              └── placement_copies (copy_id, migration_id NULL, version_id NULL, role,
                                    storage_class, blob_id, size_bytes,
                                    expected_generation, expected_class, state,
                                    verified_md5, cleanup_after, cleanup_attempts,
                                    last_error)
storage_classes (name PK, capacity_bytes, max_utilization)
```

[Миграция](migrations/20260423000000_init.sql) определяет `kind=blob/delete_marker`,
`state=pending/committed/aborted`, deferred FK указателей версий и уникальность
`(object_id, version_number)`. Наличие полей pending/delete_marker не означает
реализованные RPC finalize/abort/versioning lifecycle.

[Миграция расположений](migrations/20261026000000_version_placements.sql) создаёт
классы HOT и COLD и назначает существующим committed blob-версиям расположение в
COLD с тем же blob_id. Ёмкость классов в миграции — значения для разработки
(HOT 1 GiB × 0,9, COLD 64 GiB × 0,95); стенд эксперимента задаёт свои значения.
`versions.blob_id` остаётся blob, записанным при загрузке.
[Миграция копий](migrations/20261026010000_placement_copies.sql) добавляет
`placement_copies`. Строки копий не удаляются вместе с версией (`version_id`
становится NULL), чтобы удаление объекта во время переноса не скрыло blob.
Миграции [роли orphan](migrations/20261027000000_placement_copy_role_orphan.sql) и
[копий удалённых объектов](migrations/20261027000100_orphan_copies.sql) добавляют
роль `orphan`; только у неё `migration_id` пуст (CHECK). Добавление значения enum
вынесено в отдельный файл: PostgreSQL не даёт использовать новое значение в той же
транзакции, а CHECK второй миграции на него ссылается. Откат второй миграции
удаляет строки `orphan`, то есть забывает ещё не удалённые blobs.

Роли копий:

| Роль | Откуда | `migration_id` |
|---|---|---|
| `target` | `ReserveCopy`: новая копия при переносе | миграция |
| `source` | `SwitchPlacement`: прежний blob после переключения | миграция |
| `orphan` | `DeleteObjectMeta`: blob версии удалённого объекта | пусто |

### Регистрация версии

Текущий `CreateObjectVersion` выполняет в одной READ COMMITTED транзакции:

1. Upsert объекта по bucket/key.
2. Блокировку строки объекта и выделение `MAX(version_number)+1`.
3. Insert committed blob version с `committed_at`.
4. Insert расположения в переданном классе (по умолчанию COLD) с тем же blob_id
   и generation 1.
5. Обновление `current_version_id`.

Блокировка и INSERT должны оставаться в одной транзакции. Старые версии не
удаляются при overwrite. Повтор CreateObjectVersion создаёт новую версию:
идемпотентного operation ID пока нет, ETag его не заменяет.

Будущий coordinator сначала получает успешный Storage commit, затем регистрирует
Metadata version и только после этого возвращает внешний успех. Это целевой
межсервисный flow; атомарной транзакции между файлом и PostgreSQL сейчас нет.

### Перенос версии между классами

Инварианты протокола из диплома: у версии ровно одно действующее расположение;
оно меняется только после проверки копии; переключение — CAS по поколению и
исходному классу; источник удаляется после паузы, превышающей время чтения в
Gateway.

```text
GetPlacement ─► ReserveCopy ─► запись blob_id в Storage целевого класса
             ─► сверка размера и MD5 с источником ─► SwitchPlacement
             ─► (cleanup_after) удаление старого blob ─► CompleteCleanup
Ошибка до SwitchPlacement: AbortCopy ─► удаление целевого blob ─► CompleteCleanup
```

1. `ReserveCopy` в одной транзакции: блокирует строки объекта и расположения,
   проверяет активность объекта и допустимость переноса: текущая версия может
   перемещаться в обе стороны, нетекущая — только HOT → COLD. Поколение и исходный
   класс должны быть равны ожидаемым. Затем блокирует строку целевого класса,
   считает занятость и вставляет целевую копию
   `reserved`. Блокировка класса сериализует резервирования: без неё
   параллельные запросы переполняют HOT (проверено integration-тестом).
2. Поколение и исходный класс сохраняются в целевой копии. `SwitchPlacement`
   повторно блокирует объект и расположение и обновляет запись только при
   совпадении с ними (`UPDATE ... WHERE generation = $2 AND storage_class = $3`).
   Проверка активности и допустимости переноса повторяется под блокировками.
   Перезапись и удаление блокируют ту же строку объекта, поэтому не могут
   проскочить между проверкой и переключением. Появление новой current-версии
   после резервирования не мешает выселению HOT → COLD; перенос в HOT версии,
   ставшей нетекущей, по-прежнему отклоняется с `ABORTED`.
   При переключении сначала читается target copy, затем блокируются объект и
   расположение, затем target copy перечитывается под блокировкой. Этот порядок
   не даёт удалению (объект → FK-обновления copy rows) образовать deadlock с
   переключением (copy row → объект).
3. При успехе целевая копия становится `promoted`, прежний blob записывается
   копией-источником `cleanup_pending` с `cleanup_after = now() + пауза`.
4. Конфликт оставляет расположение и копии без изменений; исполнитель вызывает
   `AbortCopy`, и целевая копия сразу становится доступной для очистки.

Если удаление побеждает выселение нетекущей версии, её HOT blob становится
`orphan`, target остаётся `reserved` с пустым `version_id`, а `SwitchPlacement`
возвращает `ABORTED`. После `AbortCopy` target тоже доступен для очистки. Если
первым завершилось переключение, удаление создаёт `orphan` для нового COLD blob;
старый HOT blob уже учтён копией `source` в `cleanup_pending`. В обоих случаях
занятость сохраняется до подтверждённой очистки. Повторы с тем же migration_id
сохраняют прежнюю идемпотентность; запрос новой миграции для отсутствующего
расположения по-прежнему получает `NOT_FOUND`.

Состояния копии:

| Состояние | Место занято | Переходы |
|---|---|---|
| `reserved` | да | → `promoted` (SwitchPlacement), → `cleanup_pending` (AbortCopy) |
| `promoted` | учитывается расположением | конечное |
| `cleanup_pending` | да | → `deleted` или `cleanup_failed` (CompleteCleanup) |
| `cleanup_failed` | да | → `deleted` или снова `cleanup_failed` |
| `deleted` | нет | конечное |

Занятость класса = размеры всех версий с расположением в классе (включая
нетекущие) + размеры копий в `reserved`, `cleanup_pending`, `cleanup_failed`.
Допустимый объём — `floor(capacity_bytes × max_utilization)`. Неудачная очистка
остаётся видимой в `ListStorageClasses` и продолжает занимать место.

После перезапуска исполнитель восстанавливает шаг по `ListMigrationCopies`:
`reserved` — переключения не было, `promoted` и копия-источник — CAS выполнен,
осталась очистка.

### Удаление и Kafka

| Topic из development config | Key | JSON payload | Текущая доставка |
|---|---|---|---|
| `object-deleted` | object_id | `object_id`, `blob_id` | после DELETE; best-effort |
| `bucket-deleted` | bucket_id | `bucket_id`, `bucket_name` | после DB commit; best-effort |

При ошибке publish RPC возвращает ошибку, хотя данные уже удалены. Повтор не
гарантирует восстановления события. Storage/AuthZ cleanup consumers для этих
событий пока не реализованы; outbox остаётся отдельной работой.

DeleteObjectMeta в одной READ COMMITTED транзакции:

1. Блокирует строку объекта (`FOR UPDATE`). Перезапись (`CreateObjectVersion`) и
   `ReserveCopy`/`SwitchPlacement` блокируют ту же строку, поэтому ждут удаления
   и не добавляют версию или blob, который шаг 2 не увидит.
2. Для каждой версии с расположением вставляет копию `orphan` в
   `cleanup_pending` с классом и blob_id расположения, размером версии и
   `cleanup_after = now() + METADATA_DELETE_CLEANUP_DELAY`.
3. Удаляет object; versions и `version_placements` уходят каскадно.

Ошибка любого шага откатывает всё: объект остаётся, копий нет. Событие
`object-deleted` по-прежнему содержит только текущий blob_id и отправляется после
commit. Это не delete marker: после удаления версии недоступны, а место в классах
остаётся занятым копиями `orphan`, пока очистка не удалит blob из Storage и не
вызовет `CompleteCleanup(deleted=true)`. Задержка нужна, чтобы GET, начатый до
DELETE и уже получивший blob_id, дочитал файл; поэтому она обязана превышать
`GATEWAY_MAX_READ_DURATION`.

Зарезервированная целевая копия удаляемой версии остаётся `reserved` с пустым
`version_id`: blob, возможно, ещё пишется. `SwitchPlacement` такой копии вернёт
`ABORTED` (версия не текущая), после чего исполнитель вызывает `AbortCopy`, и
цель сразу попадает в очистку.

## Конфигурация

Go загружает `.env` из текущего каталога; можно передать `-config-path`. Уже
заданные переменные окружения имеют приоритет. Compose читает сервисный
[.env](.env); значения в нём относятся к локальному стенду.

| Переменные | Значение / назначение |
|---|---|
| `GRPC_HOST`, `GRPC_PORT` | обязательные; в стенде `0.0.0.0`, `50052` |
| `PG_DSN` | необязательная готовая строка подключения, приоритет над полями PG |
| `PG_HOST`, `PG_PORT_INNER`, `PG_DATABASE_NAME`, `PG_USER`, `PG_PASSWORD` | обязательный полный набор, если PG_DSN не задан |
| `PG_SSL_MODE`, `PG_TIMEOUT`, `PG_LOGGING` | defaults кода `disable`, `5s`, `false` |
| `KAFKA_BOOTSTRAP` | обязательный, Compose `kafka:29092` |
| `KAFKA_OBJECT_DELETED_TOPIC`, `KAFKA_BUCKET_DELETED_TOPIC` | обязательные; `object-deleted`, `bucket-deleted` |
| `METADATA_DELETE_CLEANUP_DELAY` | default `10m`; через сколько после удаления объекта blobs его версий становятся доступны для очистки |
| `GATEWAY_MAX_READ_DURATION` | default `5m`; тот же параметр, что в Gateway. Старт завершается ошибкой, если `METADATA_DELETE_CLEANUP_DELAY` не больше него. Compose передаёт обоим сервисам одну переменную |
| `RATE_LIMITER_LIMIT`, `RATE_LIMITER_PERIOD` | defaults `100`, `1s`, локальный limiter |
| `LOGGER_LEVEL`, `LOGGER_AS_JSON`, `LOGGER_ENABLE_OLTP` | обязательные настройки Go logger; написание **OLTP** соответствует коду |
| `OTEL_SERVICE_NAME`, `OTEL_SERVICE_VERSION`, `OTEL_ENVIRONMENT` | обязательные атрибуты telemetry |
| `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_METRICS_PUSH_TIMEOUT` | обязательные endpoint и timeout |

Парсинг и полный набор: [internal/config/env](internal/config/env). Credentials
базы должны совпадать с PostgreSQL и migrator; смена env не меняет пароль уже
инициализированного volume.

## Запуск

### Docker

Из корня репозитория:

```bash
docker compose --profile services up --build -d metadata
docker compose ps -a migrator-metadata metadata
docker compose logs -f metadata
```

Compose дожидается PostgreSQL/Kafka и успешного `migrator-metadata`.
Приложение само миграции не запускает.

### Локальный Go и зависимости в Docker

Из корня:

```bash
docker compose --profile services up --build -d migrator-metadata kafka
docker compose --profile services stop metadata
cd services/metadata
PG_HOST=localhost PG_PORT_INNER=5433 KAFKA_BOOTSTRAP=localhost:9092 \
  OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317 go run ./cmd/server
```

Перед запуском дождитесь завершения мигратора с кодом 0 и готовности Kafka.
Для экспорта telemetry поднимите `make up-observability` из корня.

## Примеры использования

`grpcurl` работает с reflection. Создание демонстрационного bucket:

```bash
grpcurl -plaintext -d '{"name":"docs-demo","owner_id":"550e8400-e29b-41d4-a716-446655440000"}' \
  localhost:50052 opens3.metadata.v1.MetadataService/CreateBucket
grpcurl -plaintext -d '{"bucket_name":"docs-demo"}' \
  localhost:50052 opens3.metadata.v1.MetadataService/GetBucket
```

Сначала сохраните blob по примеру [Storage](../storage/README.md), затем подставьте
полученные blobId/checksumMd5; размер должен соответствовать файлу:

```bash
grpcurl -plaintext -d '{"bucket_name":"docs-demo","key":"example.txt","blob_id":"<blob-id>","size_bytes":"3","etag":"<checksum-md5>","content_type":"text/plain"}' \
  localhost:50052 opens3.metadata.v1.MetadataService/CreateObjectVersion
grpcurl -plaintext -d '{"bucket_name":"docs-demo","key":"example.txt"}' \
  localhost:50052 opens3.metadata.v1.MetadataService/GetObjectMeta
grpcurl -plaintext -d '{"bucket_name":"docs-demo","max_keys":10}' \
  localhost:50052 opens3.metadata.v1.MetadataService/ListObjects
```

Перенос версии в HOT без Storage (Metadata не проверяет blobs, MD5 здесь условный).
Значение `versionId` возьмите из ответа CreateObjectVersion:

```bash
grpcurl -plaintext -d '{"bucket_name":"docs-demo","key":"example.txt"}' \
  localhost:50052 opens3.metadata.v1.MetadataService/GetObjectMeta   # storageClass COLD, generation 1
grpcurl -plaintext -d '{"migration_id":"22222222-2222-4222-8222-222222222222","version_id":"<version-id>","expected_generation":1,"source_class":"COLD","target_class":"HOT"}' \
  localhost:50052 opens3.metadata.v1.PlacementService/ReserveCopy
grpcurl -plaintext -d '{}' localhost:50052 opens3.metadata.v1.PlacementService/ListStorageClasses
grpcurl -plaintext -d '{"migration_id":"22222222-2222-4222-8222-222222222222","verified_size_bytes":3,"verified_md5":"9e107d9d372bb6826bd81d3542a419d6","source_cleanup_delay_ms":60000}' \
  localhost:50052 opens3.metadata.v1.PlacementService/SwitchPlacement
grpcurl -plaintext -d '{"bucket_name":"docs-demo","key":"example.txt"}' \
  localhost:50052 opens3.metadata.v1.MetadataService/GetObjectMeta   # storageClass HOT, generation 2
```

Повтор ReserveCopy с другим migration_id и `expected_generation: 1` вернёт
`ABORTED: placement generation or class changed`. Через минуту
`ListDueCleanups` вернёт старый blob в COLD.

Удаление demo metadata через DeleteObjectMeta не удаляет blob из Storage, но
оставляет его в учёте: после `METADATA_DELETE_CLEANUP_DELAY` он появится в
`ListDueCleanups` с `role: "orphan"`. Цикла очистки пока нет; вручную удалите blob
через Storage DeleteObject и подтвердите:

```bash
grpcurl -plaintext -d '{"bucket_name":"docs-demo","key":"example.txt"}' \
  localhost:50052 opens3.metadata.v1.MetadataService/DeleteObjectMeta
grpcurl -plaintext -d '{"limit":10}' localhost:50052 opens3.metadata.v1.PlacementService/ListDueCleanups
grpcurl -plaintext -d '{"copy_id":"<copy-id>","deleted":true}' \
  localhost:50052 opens3.metadata.v1.PlacementService/CompleteCleanup
```

## Наблюдаемость и диагностика

```bash
grpcurl -plaintext localhost:50052 grpc.health.v1.Health/Check
grpcurl -plaintext -d '{}' localhost:50052 opens3.metadata.v1.MetadataService/HealthCheck
```

Первый endpoint отражает lifecycle статус процесса; второй выполняет PostgreSQL
Ping и Kafka RefreshMetadata. TCP-доступность не доказывает готовность зависимостей.

При ошибке подключения проверьте container/host адреса; при missing tables —
логи `migrator-metadata`; при ошибке delete/publish проверьте состояние каталога
перед повтором. Structured logs, metrics и spans экспортируются через Go kit.

## Разработка и тесты

Unit tests (из сервиса):

```bash
cd services/metadata
go test ./...
```

Repository integration tests (из корня; отдельная тестовая БД):

```bash
make test-metadata-integration-local
make down-e2e
```

Без автоматического запуска контейнера: `make up-e2e`, затем
`make test-metadata-integration`. Кроме repository suites, команда запускает
[сценарии протокола переноса](internal/service/placement/tests/protocol_integration_test.go)
с настоящими транзакциями: цикл COLD → HOT → COLD, повторы команд, учёт копий в
ёмкости, конкурентные резервирования, перезапись и удаление объекта во время
переноса, [выселение нетекущих HOT-версий](internal/service/placement/tests/eviction_integration_test.go)
(в том числе при перезаписи), сериализация выселения и удаления в обоих порядках,
копии `orphan` версий удалённого объекта в HOT и COLD и сохранение
занятости до `CompleteCleanup`, [выбор класса записи](internal/service/placement/tests/write_class_integration_test.go)
(наследование HOT при перезаписи, COLD без размера и при нехватке места). Пакеты запускаются последовательно (`-p 1`),
поскольку используют одну тестовую БД. Не запускайте несколько таких команд
одновременно против этой БД. Suite пересоздаёт схему и чистит таблицы —
используйте конфигурацию [e2e](../../e2e/README.md), а не development/рабочую БД.
Каталог Planner (нетекущие HOT-версии, фильтр active, порядок страниц и время
создания): [list_integration_test.go](internal/repository/placement/tests/list_integration_test.go).
Тест выдачи version_number: [insert_version_integration_test.go](internal/repository/object/tests/insert_version_integration_test.go).

После изменения wire schema из корня выполните `make install-deps`, затем
`make generate-metadata-go` / `make generate-metadata-py` с установленным `protoc`.
Generated outputs вручную не редактируются.

## Ограничения и дальнейшие работы

- pending/finalize, delete markers и полный version lifecycle ещё не реализованы;
- outbox, version-aware GC и безопасные retries требуют отдельного протокола;
- нет межсервисной аутентификации или проверки owner_id по identity;
- prefix/pagination имеют описанные выше отличия от внешнего S3-контракта;
- проверка непустоты bucket и конкурентное создание объектов требуют согласования
  lifecycle; наличие транзакции само по себе не доказывает нужную сериализацию;
- событие `object-deleted` не содержит класс хранения и лишь текущий blob_id;
  полный список blobs удалённого объекта — копии `orphan` в `ListDueCleanups`;
- нетекущие версии активных объектов можно только выселять HOT → COLD;
  перенос нетекущей версии в HOT запрещён;
- ёмкость классов задаётся миграцией или SQL, RPC для настройки нет; адреса
  экземпляров Storage в `storage_classes` не хранятся;
- исполнитель переносов работает в SPPR ([README SPPR](../sppr/README.md#перенос-версии)),
  его цикл очистки при `MIGRATION_ENABLED=true` удаляет due-копии всех ролей
  (`source`, отменённые `target`, `orphan`) напрямую из Storage и вызывает
  `CompleteCleanup`; пауза переносов очистку не останавливает. Без запущенного
  исполнителя или при ошибке Storage копии продолжают занимать место.

## Связанные документы

- [Обзор проекта](../../README.md) и [первый запуск](../../GETTING_STARTED.md).
- [Карта документации](../../docs/README.md).
- [Правила разработки](../../AGENTS.md).
- [Проверка исправлений C01–C03](../../docs/documentation-review-2026-09-11.md).
- [План согласования Storage и Metadata](../../docs/storage-service-implementation-plan.md).
