# SPPR Service

Основа сервиса поддержки принятия решений OpenS3 на Go. Текущий срез — Ingestor
из шага 3.1 (история обращений, дедупликация, контроль пропусков и агрегаты) и
исполнитель переносов HOT/COLD из этапа 4.3: действия, воркеры, бюджет, восстановление
после рестарта и очистка blobs, расчёт признаков, часовых меток и EWMA из шага 6,
а также Planner этапа 7: план размещения на EWMA или LightGBM с объяснениями,
ревизии и команды администратора.

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
  после установленной Metadata задержки, включая повтор после ошибки удаления;
- при `PLANNER_ENABLED=true` по таймеру и по команде считает ревизию плана: прогноз
  EWMA или LightGBM по минутным агрегатам, жадный выбор по формулам ВКР с
  ёмкостью, бюджетом,
  закреплениями, минимальным интервалом и порогом выигрыша Δmin, объяснение
  каждой рекомендации и отказа; администратор подтверждает план целиком или
  допустимую часть, и подтверждённые действия уходят исполнителю.

Feature Builder и EWMA работают и в автономном CLI (датасеты шага 6), и внутри
Planner. Доступ через Auth/AuthZ и HTML-панель реализованы. Go-предиктор LightGBM
загружает файловый артефакт; обучение и оценка качества относятся к части 2 этапа 8.
Этап 9 добавляет реестр, переобучение и горячую замену модели. Цикл очистки старых
blobs работает в том же процессе; проверки сбоев и `make smoke-migration`
описаны ниже.

Названия: «минимальный интервал» — пауза между переносами одной версии
(`min_interval_seconds` в API переносов; раньше в документах его называли Δmin).
Δmin в Planner и в тексте ВКР — порог выигрыша набора переносов, мс.

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
cmd/dataset/                      учебный пример, синтетика и внешняя история
cmd/trace-convert/                локальные Meta CDN / IBM COS / Docker → формат шага 6
internal/experiment/              генератор и сборка выборки по логическому времени
internal/service/features/        общие правила признаков до T
internal/service/predictor/ewma/   оценка числа чтений за горизонт
examples/data/                    small и acceptance профили генератора
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
internal/model/                   события, ErrRejected, действия переноса, признаки, прогнозы и ошибки
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
воркеров. Доступ защищён при `ADMIN_AUTH_ENABLED=true` (по умолчанию вместе с
Planner); при выключенной проверке это локальный тестовый API с warning.
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
Gateway producer из независимого [PR #1](https://github.com/WithSoull/s3-sppr/pull/1)
уже есть в checkout и публикует успешные GET/PUT при `ACCESS_EVENTS_ENABLED=true`.
Ingestor можно запускать и тестировать без producer: достаточно Kafka и PostgreSQL.

### HTTP API Planner

JSON на том же порту, префикс `/api/planner`, только при `PLANNER_ENABLED=true`.
Как и `/api/migration`, проходит через `access.Middleware`: Auth проверяет
access token, AuthZ — право ADMIN на `system:placement`. Инициатор команды —
строковый UUID `user_id` из проверенного JWT, а не `sub`.

| Метод | Путь | Назначение |
|---|---|---|
| GET | `/status` | занятость HOT/COLD, качество истории, метод прогноза, пауза, бюджет, последняя ревизия, число `pending`/`running` действий |
| GET | `/plans?page_size=&page_token=` | ревизии от новых к старым; `page_size` ≤ 100 |
| GET | `/plans/latest`, `/plans/{id}` | ревизия: статус, триггер, параметры расчёта, сводка, причина |
| GET | `/plans/{id}/recommendations?selected=&kind=&page_size=&page_token=` | рекомендации в порядке плана с объяснением и `depends_on`; отклонённые кандидаты идут после выбранных |
| POST | `/plans/recompute` | внеочередной расчёт; ответ сразу, план появится в `/plans/latest` |
| POST | `/plans/{id}/confirm` | `{command_id, recommendation_ids?}`; пусто — весь план |
| POST | `/plans/{id}/reject` | отклонить последнюю готовую ревизию |
| POST | `/commands/cancel-pending` | `{command_id, plan_id}` или `{command_id, action_ids}` |
| POST | `/commands/pause`, `/commands/resume` | пауза исполнителя |
| GET | `/commands?plan_id=&page_size=&page_token=`, `/commands/{id}` | журнал решений администратора |
| GET / PUT | `/settings` | `{horizon_seconds, hot_fixed_ms, hot_mib_per_ms, cold_fixed_ms, cold_mib_per_ms, penalty_ms_per_mib, min_gain_ms, hot_utilization, cold_utilization}`; PUT — команда с `command_id` |

Каждая команда несёт `command_id` (UUID клиента). Повтор с теми же полями
возвращает первый результат, с другими — 409. Принятая команда — 202 с телом
команды. Команда, которую правила отклонили, сохраняется в журнале и
возвращается с 409 и кодом: `PLAN_STALE` (не последняя ревизия или неподходящий
статус), `DEPENDENCY_REQUIRED` (выбрано действие без предшественника, который
освобождает для него место; `missing` перечисляет недостающие). План не
дополняется автоматически. Предшественник, отправленный исполнителю прошлым
подтверждением того же плана, недостающим не считается.

### Доступ и HTML-панель

При `PLANNER_ENABLED=true` проверка доступа включена по умолчанию. Токен
передаётся в `Authorization: Bearer <access token>` либо cookie `sppr_access`.
SPPR вызывает `AuthV1.ValidateToken(Empty)` с gRPC metadata
`authorization: Bearer …`, затем читает JWT `user_id` (UUID), `token_type=access`
и срок `exp`. До успешной проверки Auth payload не используется. Следом
`PermissionService.Check` получает `subject=user:<user_id>`, `ACTION_ADMIN`,
`object=system:placement`. Этот идентификатор поддерживается текущим AuthZ:
servicer не ограничивает префикс, `infer_node_label` относит его к `Resource`.
Это отдельный административный ресурс, создавать S3 bucket для него не нужно.
AuthZ не изменён. Его комментарии proto перечисляют примеры `bucket:`/`object:`,
но runtime принимает и `system:`.

Без токена или с неверным/истёкшим токеном JSON API отвечает **401**, без ADMIN —
**403**, с кодом `ACCESS_DENIED`. Ошибка/таймаут Auth или AuthZ даёт **503**:
доступ не открывается при сбое. `/healthz` и `/readyz` остаются открытыми;
проверка `/readyz` по-прежнему относится к БД, не к Auth/AuthZ.

Откройте `http://localhost:8086/admin`. `GET/POST /admin/login` обменивает
email/password через `Auth.Login` → refresh token → `GetAccessToken`; refresh
не сохраняется. После проверки ADMIN access token записывается в HttpOnly
cookie, `SameSite=Strict`, `Path=/`, со сроком JWT; при HTTPS также `Secure`.
`POST /admin/logout` удаляет cookie. После истечения токена HTML перенаправляет
на логин; автоматического продления сессии нет. Bearer имеет приоритет над cookie.
HTML и ответы входа имеют `Cache-Control: no-store`.

Страницы (Go `html/template`, встроены через `embed.FS`, без JavaScript):

- `/admin`: суммарная занятость классов с копиями и целями pending, качество
  истории, метод, бюджет/пауза/интервал, последняя ревизия, pending/running;
- `/admin/settings`: все коэффициенты Planner, бюджет/окно/интервал исполнителя,
  пауза и закрепления; автор закрепления берётся из проверенного пользователя;
- `/admin/recommendations`, `/admin/plans`: последняя и сохранённые ревизии,
  фильтры selected/kind, страницы по 50 строк, карточки объяснений и зависимостей;
  чекбоксы подтверждают только отмеченные строки текущей страницы, отдельная
  кнопка подтверждает весь план; пустой выбор не подтверждает весь план;
- `/admin/commands`, `/admin/commands/{id}`: принятые и отклонённые команды,
  человекочитаемая причина и следующий шаг, недостающие зависимости, ссылки
  на действия; `/admin/migrations?plan_id=…`, `/admin/migrations/{id}` — действия
  и журнал переходов/байт/очистки из MigrationService.

Ожидаемый эффект карточки визуально отделён от фактических результатов журнала.
Формы изменений несут UUID `command_id`; повтор возвращает сохранённый результат,
а изменение полей под тем же UUID даёт 409. Команды Planner используют его
существующий журнал; бюджет, интервал и pins — новую `admin_form_commands`,
запись результата и эффект одной транзакцией с блокировкой concurrent retry.
Эти два журнала показаны отдельно на странице «Команды». Примените все SQL
миграции, включая `20261030000000_admin_forms`. Вход/выход не являются командами
размещения и не сохраняют пароль или токены в журнале.

CSRF: SameSite=Strict и запрет изменяющих запросов с чужим host в Origin **или**
Referer (также login/logout и cookie JSON API). CLI без обоих заголовков разрешён.
Необходим прямой host панели в этих заголовках; при reverse proxy сохраняйте Host.


## Данные и основные сценарии

Автономный Feature Builder считает окна 5 мин / 1 ч / 6 ч / 24 ч, давность двух
последних чтений, сглаживание с полураспадами 1 ч и 6 ч, динамику, суточный сдвиг,
долю прочитанных байтов, признаки бакета, размера, возраста и перезаписей.
История передаётся после дедупликации Ingestor. Используются только успешные
клиентские чтения строго до T. Метка отдельно считает чтения в `[T,T+1ч)`.
Расчёт доступен через `service.FeatureBuilder` и service provider; периодический
вызов в работающем сервере пока не подключён. Срезы сохраняет CLI в JSONL (default) или CSV.gz по split.
Детальные правила, качество данных и маршрут чтения — в
[шаге 6](../../docs/sppr-step6.md).

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

`last_event_at` — прогресс одного потока, а не общая W. `StreamHeartbeat`
с заголовком `event-type: heartbeat` задаёт `complete_until`; хвостовые пропуски
сохраняются атомарно с прогрессом и hash receipt. Открытый поток без heartbeat
дольше `INGEST_STREAM_LOST_AFTER` закрывается как потерянный с gap неизвестного
размера (`last_sequence IS NULL`). Первый номер больше 1 создаёт пропуск с
неизвестной нижней границей времени. Старые gaps автоматически не закрываются;
поздние события из них и продолжение закрытого потока отклоняются. Полный
протокол — [спецификация](../../docs/history-completeness-spec.md).

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
| `migration_settings` | одна строка: бюджет (байт за окно), минимальный интервал, пауза; при первом старте из env |
| `migration_pins` | закрепления bucket/key — «не переносить» |

Воркер берёт действие в одной транзакции: блокирует строку `migration_settings`
(конкурентные взятия идут по очереди и не превышают бюджет), считает списания за
окно и выбирает самое старое `pending`, все зависимости которого `succeeded` с
удалённым источником (`source_cleaned_at`, см. «Planner»), а размер помещается в
остаток, с `FOR UPDATE SKIP LOCKED`. В той же транзакции
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

### Planner

Цикл (`service/planner`) выполняется при старте, раз в `PLANNER_INTERVAL` и по
команде `recompute`; команды, пришедшие во время расчёта, получают следующую ревизию.

1. Снимок на момент T: каталог Metadata `ListPlacements(include_noncurrent_hot)`
   (текущие версии и перезаписанные версии в HOT), `ListStorageClasses`,
   закрепления, минимальный интервал, остаток бюджета и действия `pending`/`running`.
   Ещё не зарезервированные цели принятых действий добавляются к занятости
   класса, а их размер вычитается из бюджета.
2. История: события за 24 ч до W группируются в SQL по минутам
   (`occurred_at < W` до группировки, чтобы исключить будущие чтения текущей минуты)
   → Feature Builder → выбранный `service.Predictor` → `N̂`.
   Доля чтения `r` — `mean_read_fraction`, без чтений 1.
3. Полнота: расчёт идёт на `at = W`, общем минимуме границ открытых потоков.
   Если открытых нет — максимум границ закрытых, не позже последнего закрытия;
   без потоков W неизвестна. Начало истории расширяется до самой старой версии
   каталога. Неизвестная полнота, пропуск в окне, версия новее W или отставание
   больше `PLANNER_MAX_HISTORY_LAG` дают `history_incomplete` без рекомендаций;
   отставание сохраняется в причине и показывается в статусе.

4. `planner.Build`: `L = N̂·c + D̂/v`, `Δ = L_COLD − L_HOT`. Сначала переполненный
   HOT разгружается (`rebalance`). Потом кандидаты COLD берутся по убыванию `Δ/s`.
   Если места нет, сравниваются два варианта вытеснения: объекты HOT по
   возрастанию `Δ/s` в пределах бюджета и один объект с наименьшей потерей,
   которого хватает. Освобождённое место дозаполняется следующими кандидатами.
   Набор принимается, если `ΣΔ − ΣΔ_вытесн − P·копируемое ≥ Δmin`, хватает бюджета
   и места в COLD. Перенос в HOT зависит от своих вытеснений. Остаток бюджета
   выселяет перезаписанные версии из HOT (`evict_noncurrent`).
5. Объяснение строится из тех же величин: прогноз и признаки, `L`, `Δ`, штраф,
   чистый выигрыш набора, вытесняемые, занятость классов и бюджет до и после,
   причина отказа (`pinned`, `min_interval`, `in_progress`, `budget`,
   `hot_capacity`, `cold_capacity`, `gain_below_threshold`, `no_forecast`).
   Сохраняются выбранные и не больше `PLANNER_MAX_REJECTED` лучших отклонённых.

#### Выбор предиктора и каталог модели

ModelManager выбирает активную модель из журнала `model_versions` и каталога
`PLANNER_MODEL_REGISTRY/<model_version>`. При пустом журнале непустой
`PLANNER_MODEL_DIR` импортируется как `bootstrap`; иначе до успешного обучения
используется EWMA. `PLANNER_PREDICTOR` оставлен как совместимый вход конфигурации
(`ewma|lgbm`), но выбор в этапе 9 определяется журналом и сроком годности.
Каталог содержит `manifest.json`, `classifier.txt`, `regressor.txt`, `parity.json`;
точная схема — [контракт модели](../../docs/ml-spec.md#контракт-модели-общий-для-частей-1-и-2).
Загрузчик проверяет SHA-256 обоих файлов, `method_version=lgbm-hurdle-v1`,
`feature_rules_version=features-v1`, все 17 имён и их порядок в манифесте и моделях,
`horizon_seconds` = сохранённый горизонт Planner, а затем контрольные строки Python с относительным
допуском `1e-9` для сырых выходов, `p`, `m`, `N̂` (для нулевого эталона — точный ноль).
В Go применяются явные `sigmoid` и `exp`: `p = sigmoid(raw_classifier)`,
`m = 1 + exp(raw_regressor)`, `N̂ = p·m`. `null` в контрольных признаках и
отсутствующая давность чтения становятся NaN для LightGBM.

Ошибка загрузки оставляет сервис работающим на EWMA. `/api/planner/status` и
`/admin` показывают активные `method_version`, `model_version` (пустая строка для
EWMA) и `fallback_reason`. Вложенный `model_lifecycle` содержит сведения об обновлении.
Неверное значение `PLANNER_PREDICTOR` является ошибкой конфигурации.
Ревизия плана сохраняет метод и версию артефакта; карточка ML-рекомендации
добавляет `p`, `m`, `N̂` и «оценку EWMA для сравнения» на том же горизонте
(в JSON — `forecast.hurdle`, а `N̂` — `forecast.expected_reads`). EWMA-карточка
сохраняет прежние поля. NaN/Inf/отрицательный прогноз становится `no_forecast`
для объекта; это не переключает предиктор всего процесса.

Настройки в БД имеют приоритет над env, в том числе горизонт. В начале цикла
Planner захватывает один неизменяемый предиктор; горячая замена посередине цикла
не меняет ни прогнозы, ни записанную версию плана. После смены горизонта через
панель несовместимая активная модель выводится с причиной `horizon_changed`,
Planner использует EWMA; очередное обучение берёт новый сохранённый горизонт.

Перед активацией выполняются `lgbm.Load` и прогнозы на последних признаках
каталога: любой неконечный/отрицательный выход отклоняет кандидата. Нет строк
для пробного прогноза, нет корректных временных интервалов обучения или метки
заканчиваются в будущем — также отказ. Каталоги уже записанных версий не
перезаписываются. Ошибка обучения/проверки сохраняет текущую модель. Возраст
`now - trained_to > PLANNER_MODEL_MAX_AGE` выводит её с причиной `model_stale`;
следующая успешная попытка снова включает ML. При рестарте восстанавливается
только активная запись; прерванные `candidate` получают `training_interrupted`.

Compose сохраняет реестр в именованном томе `sppr-model-registry`
(`/models/registry`, запись), а `./build/models:/models:ro` используется для
импорта. Финальный образ содержит Python 3.12, `sppr_ml`, закреплённые зависимости
и `libgomp`; квота процесса вместе с обучением — 2 CPU / 2 GiB,
`num_threads=2`. Измерение фактического пика памяти этим ограничением не заменяется.

```bash
# Только для пустого журнала; build/models/current содержит четыре файла.
PLANNER_MODEL_DIR=/models/current make up-sppr-planner
# Ручное обучение; токен администратора, полученный через Auth.
curl -X POST http://localhost:8086/api/planner/model/retrain \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"command_id":"11111111-1111-4111-8111-111111111111"}'
```

Попытка запускается по расписанию и вручную; одновременно выполняется одна.
Команда с повторным `command_id` возвращает прежний результат без нового
обучения; отказ (`training_in_progress`, `insufficient_history`, причина Coverage)
также попадает в `admin_commands`. До достаточной полной истории запись в
`model_versions` не создаётся. Принятая команда сохраняет `candidate` в той же транзакции, что и
`admin_commands`, и ставит работу в очередь процесса. При аварии до запуска
процесса кандидат получает `training_interrupted`; новая попытка требует
расписания или нового `command_id`.

`dataset.FromHistory` вызывается runner внутри Go: Metadata ListPlacements,
минутные агрегаты и только настоящий `history.Coverage`. `assumeCaughtUp` не
используется. Срезы раз в 15 минут с минутными границами за последнее окно;
признаки берутся тем же `features.Build` из последних 24 ч, метки — `[t,t+H)`
только при пригодных признаках/метках и `W >= t+H`. Последняя 1/7 окна —
validation, остальное — train с исключением меток через границу; test пуст.
Пишутся CSV.gz и manifest, которые читает Python. Удалённые версии и старые
COLD, отсутствующие в сегодняшнем каталоге Metadata, в набор не попадут:
это ограничение записано в manifest. Возраст модели считается до последней
реально выписанной метки (`last_label_available_at`), а не до времени запуска.
Пока этап 5 не поставляет достоверную Coverage, обучающий контур не предполагает
полноту по молчанию. Для произвольных секунд минутные метки неточны:
runner отказывает с `horizon_not_whole_minutes`, прогноз на EWMA остаётся доступен.

Временный набор и выход процесса находятся внутри реестра и удаляются после
попытки. Сохраняются четыре файла артефакта и строка журнала. Новая версия
включается после проверки и транзакционного вывода предыдущей версии;
качество `validation_cov_k` / `validation_ewma_cov_k` записывается для сведения.
Команда обучения получает безопасно экранированные `{data}`, `{out}`, `{params}`
(подставлять без дополнительных кавычек); сама команда — доверенная конфигурация
оператора. Таймаут охватывает подготовку и процесс; отмена убивает группу процессов.
Параметры берутся из активного manifest без сетки, иначе используются
`num_leaves=15, learning_rate=0.03, min_data_in_leaf=20, lambda_l2=1.0`.
Раздельные вложенные параметры classifier/regressor текущий Python trainer
не поддерживает: попытка отклоняется с причиной, параметры не угадываются.

`GET /api/planner/status` → `model_lifecycle`: `active` (версия и источник),
`age_seconds`, `max_age_seconds`, `fallback_reason`, `last_attempt` (время,
статус, причина), `next_attempt`, `training`, `progress`, `history_reason`,
`journal_error`. Эти же сведения и кнопка обучения есть в `/admin`.
`/admin/models` и `GET /api/planner/model/versions` показывают журнал по 100 записей
с переходом `before=<RFC3339Nano>`; каталоги отклонённых моделей не удаляются.

Для прямого запуска Go укажите путь на хосте. Env для Compose передавайте из
shell или корневого `.env`; `services/sppr/.env.example` описывает прямой запуск.
Фикстура `internal/service/predictor/lgbm/testdata` — модель из spike
`claude/wonderful-proskuriakova-bbe2b4`, а не обученная и принятая модель этапа 8.
Она проверяет совместимость Go/Python, но не качество. Её исходные интервалы
обучения не записаны (`start/end: null`), validation не проводилась (`{}`).
Фикстура без временных интервалов не принимается ModelManager; для bootstrap нужен полный экспорт Python.

Для равных размеров и Δmin = 0 план совпадает с точным перебором (2000 случайных
задач в тестах). Для разных размеров это эвристика: на тех же тестах в среднем
97,7 % оптимума, в 89 % задач оптимум. При 10 000 объектов и сутках истории
при 20 чтениях/с (670 тыс. минутных строк) расчёт занимает 0,2 с и до 172 МиБ
кучи (`SPPR_PLANNER_SCALE=1 go test -run TestPlanningScale ./internal/service/planner`).

| Таблица | Назначение |
|---|---|
| `plans` | ревизия (`revision` растёт), триггер, метод и версия модели, статус (`computing`, `ready`, `history_incomplete`, `failed`, `superseded`, `confirmed`, `rejected`), параметры, сводка, причина |
| `recommendations`, `recommendation_deps` | действия и отклонённые кандидаты с объяснением JSON; `recommendation_id` становится `action_id` |
| `admin_commands` | каждое решение администратора, включая отклонённые: инициатор, запрос, статус, код, результат |
| `model_versions` | каждая начатая попытка, active/candidate/rejected/retired, источник, интервалы, строки, длительность, метрики и даты |
| `planner_settings` | параметры модели; при первом старте из env, дальше из `PUT /settings` |

Новая ревизия переводит старые `ready` в `superseded`; подтвердить можно только
последнюю. Подтверждение одной транзакцией отправляет действия в
`MigrationService.SubmitActions`, переводит план в `confirmed` и пишет команду.
Хранятся последние `PLANNER_KEEP_PLANS` неподтверждённых ревизий; подтверждённые
не удаляются. Ревизия, оставшаяся `computing` после рестарта, помечается `failed`.

Зависимое действие исполнитель берёт, когда предшественник `succeeded` и его
источник удалён: до удаления старая копия занимает место, которое освобождает
вытеснение. Цикл очистки спрашивает Metadata, удалён ли источник, и ставит
`source_cleaned_at`, поэтому потерянный ответ или рестарт не блокирует зависимые.

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
| `INGEST_STREAM_LOST_AFTER` | `30s`, положительный; проверка каждые timeout/6 |
| `PLANNER_MAX_HISTORY_LAG` | `60s`, положительный; равенство порогу допустимо |
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
| `ADMIN_AUTH_ENABLED` | наследует `PLANNER_ENABLED`; `false` только для локальных тестов, в логе warning об открытом API |
| `AUTH_GRPC_ADDR`, `AUTHZ_GRPC_ADDR` | обязательны при включённом доступе; Compose: `auth:50050`, `authz:50051` |
| `AUTH_RPC_TIMEOUT`, `AUTHZ_RPC_TIMEOUT` | `5s`, положительные; Auth timeout покрывает обмен Login → GetAccessToken |
| `PLANNER_ENABLED` | `false`; цикл планирования и `/api/planner`. Требует `MIGRATION_ENABLED=true` |
| `PLANNER_INTERVAL`, `PLANNER_KEEP_PLANS`, `PLANNER_MAX_REJECTED` | `300s`, `200`, `1000` |
| `PLANNER_PREDICTOR`, `PLANNER_MODEL_DIR` | legacy `ewma|lgbm`; пустой путь bootstrap по умолчанию, импорт только в пустой журнал |
| `PLANNER_MODEL_REGISTRY` | `/models/registry`; writable каталог, Compose использует постоянный именованный том |
| `PLANNER_MODEL_MAX_AGE`, `PLANNER_MODEL_UPDATE_EVERY` | `72h`, `24h`; возраст от конца меток и период попыток |
| `PLANNER_MODEL_MIN_HISTORY`, `PLANNER_MODEL_TRAIN_WINDOW` | `26h`, `168h`; полная история и окно срезов |
| `PLANNER_MODEL_TRAIN_TIMEOUT` | `30m`; подготовка + subprocess + проверка |
| `PLANNER_TRAIN_COMMAND` | `python -m sppr_ml train --data {data} --out {out} --params {params}` |
| `PLANNER_HORIZON` | `1h`; горизонт H, целые секунды |
| `PLANNER_HOT_C_MS`, `PLANNER_HOT_V_MIB_PER_MS` | `1`, `0.5`; постоянная часть и скорость чтения HOT |
| `PLANNER_COLD_C_MS`, `PLANNER_COLD_V_MIB_PER_MS` | `20`, `0.05`; то же для COLD. Заглушки до измерения на стенде (этап 10) |
| `PLANNER_PENALTY_MS_PER_MIB`, `PLANNER_MIN_GAIN_MS` | `2`, `100`; штраф P и порог выигрыша Δmin |
| `PLANNER_HOT_UTILIZATION`, `PLANNER_COLD_UTILIZATION` | `0.9`; Planner берёт меньшее из этого и `max_utilization` Metadata |
| `METADATA_GRPC_ADDR`, `METADATA_RPC_TIMEOUT` | `localhost:50052`, `5s` |
| `STORAGE_HOT_GRPC_ADDR`, `STORAGE_COLD_GRPC_ADDR` | `localhost:50056`, `localhost:50053`, как у Gateway |

Параметры sarama — константы `config/env/sarama.go`: `OffsetOldest`,
`Consumer.Return.Errors=true` (ошибки группы читаются и логируются). Отдельного
лимита на попытку нет: `ExecContext` shared PG client ограничен `PG_TIMEOUT`.
Без `PG_DSN` компоненты подключения кодируются как PostgreSQL URL: пробелы и
спецсимволы в пароле не меняют смысл остальных параметров.

Бюджет, окно и минимальный интервал из env записываются только при первом старте;
дальше действуют значения, заданные через API. Окно и интервал хранятся в целых
секундах. Параметры Planner ведут себя так же.
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

Planner вместе с исполнителем, Auth, AuthZ, Users и их зависимостями:

```bash
make up-sppr-planner
curl -s http://localhost:8086/api/planner/status | jq
curl -s -X POST http://localhost:8086/api/planner/plans/recompute -d "{\"command_id\":\"$(uuidgen)\"}"
curl -s http://localhost:8086/api/planner/plans/latest | jq '{plan_id, revision, status, summary}'
```

Миграции выполняет `migrate/migrate:v4.19.1` отдельно от приложения. Приложение
проверяет таблицы перед запуском consumer. Формат миграций этого сервиса —
`<version>_<name>.up.sql` / `.down.sql`. Не запускайте их через Goose: другие
сервисы используют другой формат. Повторное `up` без новых миграций безопасно.

## Примеры использования

Без инфраструктуры, из корня репозитория:

```bash
make demo-sppr-data
make dataset-sppr
make dataset-sppr SPPR_SCENARIO=shift SPPR_SEED=1 SPPR_DATA_OUT=build/sppr-shift-1
```

Каталог назначения должен отсутствовать. В `build/sppr-data` сохраняются
`catalog.json`, `events.jsonl`, `slices.jsonl` и `manifest.json` с конфигурацией,
границами train/validation/test, версиями и SHA-256. Учебный профиль содержит
20 версий, 0,01 чтения/с, часовые срезы и появление/удаление объектов.
`examples/data/acceptance.json` фиксирует 10 000 объектов по 1 МиБ, 20 чтений/с
и срезы раз в 300 с; это большой профиль для отдельного запуска, а не результат
уже проведённого эксперимента. Шесть сценариев и все параметры описаны в
[разборе шага 6](../../docs/sppr-step6.md).

Внешние трассы (только уже доступные локальные файлы) преобразует
`go run ./services/sppr/cmd/trace-convert`: Meta CDN CSV/zstd (default) или
`-source ibm-cos` (tar gzip/Unix compress либо text) → `catalog.json`,
`events.jsonl`, manifest с URL, лицензией, SHA-256, mapping и долями выборки.
Обязательны `-input`, `-source-url`, `-source-share`, `-out` и явный
`-allow-inferred-success`: в Meta нет HTTP status, GET с телом — только прокси
успеха. Эти прежние flags относятся к Meta. Для COS обязательны `-input`,
`-bucket ibm-cos-001`, `-origin <RFC3339>`, `-out`, `-allow-inferred-success`;
календарное время искусственное. GET/range — одно чтение, PUT создаёт версию,
DELETE закрывает её; HEAD не является demand, COPY исключён из-за неясной роли ID.
Реальные COS `.tgz` используют Unix compress: нужен `/usr/bin/gzip`.
[Аудит трасс](../../docs/traces.md) содержит mapping, измерения и команды.
Docker: `-source docker -zone syd01 -input <tar.gz> -out <new-dir>`;
необязательные `-from` / `-to` фильтруют даты **имён файлов**, to exclusive.
Bucket по умолчанию `docker-<zone>`, key — digest с дедупликацией между repo;
размер — max полного GET 200, demand — GET blobs 200/206. Inferred-success opt-in
не нужен. Сортировка на диске через `/usr/bin/sort` требует временного места
пропорционально выбранным GET. Лицензия данных: `not stated; permission requested
from authors`. Go-конвертер Meta использует уже имеющийся zstd, Python/requirements
не нужны. Подтверждённые счётчики / политики конфликтов — в аудите трасс.

CLI `cmd/dataset` принимает `-catalog`, `-events`, `-start`, `-end` вместо
`-config`. Границы покрытия задаются явно, не выводятся из последнего события.
Для истории не в семь суток обязательны `-train-end` и `-validation-end`;
интервалы и их доли сохраняются до обучения. Соседний manifest конвертера
включается в результат с проверкой хешей (`-source-manifest` переопределяет путь).
`-format csv.gz` пишет train/validation/test/purged.csv.gz с идентификаторами,
всеми `features-v1`, `label_reads`, `label_has_read`, `eligible`, `split`,
`ewma_forecast` и качеством; null — пустая ячейка. `-train-every 15m` прореживает
только train, `-snapshot-every 5m` задаёт базовую сетку. Эти флаги доступны и для
синтетики; JSONL остаётся default. Полные команды и ограничения реконструкции —
в [шаге 6](../../docs/sppr-step6.md#внешние-трассы-и-компактные-срезы).
Срезы пишутся потоково, в памяти один batch каталога и ограниченная история,
а не все 20 160 000 строк профиля приёмки; полная ресурсная квота ещё не измерена.

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

Примеры ниже рассчитаны на migration-only профиль с `PLANNER_ENABLED=false`
и выключенной проверкой доступа. На защищённом стенде добавьте к curl
`-H "Authorization: Bearer $ACCESS_TOKEN"` либо используйте панель.


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

### Вход и smoke Planner

Из корня (Go нужен для seed и smoke; `.env.example` содержит тестовые значения):

```bash
make up-sppr-planner
make seed-planner-admin
# Откройте http://localhost:8086/admin
make smoke-planner SMOKE_PLANNER_ARGS="-timeout 15m -objects 3"
# Быстрый выделенный стенд с лимитом чтения 2s и очисткой 5s:
make smoke-planner-local
```

Seed вызывает `UserV1.ValidateCredentials`/`Create`, затем Login/GetAccessToken
и `AuthZ.WriteTuple(HAS_PERMISSION, ADMIN, system:placement)`. Повтор с теми же
credentials использует пользователя; чужой существующий пароль не заменяет.
Он намеренно печатает использованные **тестовые** email/password:
`planner-admin@example.test` / `PlannerAdmin123!` по умолчанию. Overrides:
`PLANNER_ADMIN_EMAIL`, `PLANNER_ADMIN_PASSWORD`, `USERS_GRPC_ADDR` (localhost:50054),
`AUTH_GRPC_ADDR` (localhost:50050), `AUTHZ_GRPC_ADDR` (localhost:50051).
Env надо экспортировать в shell; seed не читает сервисный `.env` автоматически.
После выдачи права ранее закешированный отказ AuthZ может жить до 30 s;
smoke ждёт фактический допуск. Токены в stdout не выводятся.

Smoke требует **отдельный стенд: пустой HOT, нет pending/running**, producer
Gateway включён, история без gaps, исполнитель и Planner активны, авторизация
включена. Он создаёт свой bucket и N объектов COLD по 64 КиБ, читает первый,
задаёт известные коэффициенты модели, нулевые порог/штраф и интервал,
уменьшает HOT через настройки до 1,5 размера объекта, получает и подтверждает
перенос. Затем читает второй чаще: замена требует вытеснения первого в COLD.
Проверки: 401 без токена на обоих API; 403 подтверждения non-admin (C-04);
старый план → PLAN_STALE (C-03); выбор promotion без eviction →
DEPENDENCY_REQUIRED с missing (C-02); действия succeeded и удаление HOT-источников обязательных вытеснений;
placement HOT и те же байты через Gateway; обратный перенос остывшей версии;
проверенные связи рекомендация → команда с user_id → action journal, включая
HTML-страницы (C-06). Качество прогноза или реальное ускорение не измеряются.

Изменённые настройки Planner/бюджет/окно/интервал/пауза восстанавливаются даже
при ошибке; ошибка восстановления также завершает smoke ненулевым кодом.
Bucket, объекты, тестовые пользователи и журналы остаются для просмотра.
Не запускайте параллельно другой smoke или администратора, меняющего настройки.
Таймаут включает задержку удаления HOT-источника предшественника
(обычно 10m, в local — 5s); остальные источники могут ещё ожидать фоновой очистки.
Флаги: `-gateway`, `-sppr`, `-metadata`, `-objects` (2…100), `-timeout`.

При `PLANNER_ENABLED=false` старые локальные migration-only команды по-прежнему
работают без Auth; SPPR явно предупреждает об этом. Для защищённого migration-only
запуска задайте `ADMIN_AUTH_ENABLED=true` и адреса Auth/AuthZ; старый
`smoke-migration` не отправляет токен — для защищённого полного цикла используйте
`smoke-planner`.

### Полнота истории и smoke-history

Миграция `20261101000000_history_completeness` следует после всех существующих
версий. Она добавляет границы/состояния потоков и `ingestion_heartbeats` для
идемпотентных повторов. Старые потоки не получают выдуманную полноту: до heartbeat
они удерживают W неизвестной, затем работают обычные heartbeat/lost-check.
Время первого heartbeat отдельно сохраняется для пустых потоков; созданный
позже W объект не допускает планирования до следующего барьера.
Down удаляет heartbeat receipts и новые поля. Если существуют gaps с неизвестным
числом событий, downgrade отказывается выполняться: старая схема не может их
представить. Доказательства потерь не удаляются и конечное число не выдумывается;
события и обычные gaps сохраняются.

`/api/planner/status` и `/admin` показывают `history.watermark`, `known`,
`open_streams`, `closed_streams`, `lost_streams` (подмножество закрытых), `gaps_24h`
и `history_lag_seconds`. Карточки `objects` показывают «чтений нет» только при
пригодной истории; при gap, неизвестной границе или чрезмерном lag — «история
неполна» и конкретную причину качества. Каталог новее W остаётся непригодным до
доставки heartbeat. Число gaps за 24 ч считается по `before_time`.

На **отдельном** пустом Compose стенде из корня:

```bash
make smoke-history-local
```

Цель запускает Planner/executor и identity stack, включает события, heartbeat
раз в 1 с и lost timeout 30 с. `ADMIN_AUTH_ENABLED=false` используется только на
этом локальном тестовом стенде. Smoke создаёт уникальный bucket и два объекта;
проверяет GET + рост W без чтений, byte-for-byte повтор события без роста спроса,
GET при остановленной Kafka и gap после возврата, kill Gateway + lost gap + рост
W после рестарта, graceful stop без нового gap и два состояния истории объекта.
После проверок (и при ошибке) пытается запустить Kafka/Gateway обратно.
Smoke не запускает отдельный конкурирующий executor.

Для уже работающего выделенного стенда:

```bash
SPPR_SMOKE_ACCESS_TOKEN='<access token>' make smoke-history
# Настройки CLI:
make smoke-history SMOKE_HISTORY_ARGS='-timeout 5m -outage 12s'
```

DSN берётся из `SPPR_SMOKE_PG_DSN` (по умолчанию локальный sppr_db:5436 в Makefile).
Нужны Docker CLI/socket, PostgreSQL, Kafka с доступными host listeners, Metadata,
Gateway и SPPR с Planner. `-compose-file`, `-gateway`, `-sppr`, `-metadata`,
`-kafka`, `-topic`, `-group` переопределяют адреса/имена. Kafka outage с учётом
времени docker stop/start должен быть короче lost timeout; при иных retry/buffer
настройках подберите `-outage`, чтобы producer успел зафиксировать потерю.
Результат smoke подтверждён только после фактического прогона всех PASS.

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

### Smoke обновления модели

```bash
make smoke-ml-update-local
# Уже запущенный выделенный стенд с теми же env:
make smoke-ml-update
```

Нужны полный identity stack, Gateway producer, executor и достоверная Coverage
этапа 5. HOT пуст, нет pending/running. Профиль: `PLANNER_HORIZON=2m`, сохранённый
горизонт также 120, `PLANNER_MODEL_MIN_HISTORY=1m`, окно `140m`, расписание `24h`.
Фиксированный шаг 15m требует не меньше 35m реальной истории; по умолчанию
CLI читает 40m (`SMOKE_ML_UPDATE_ARGS='-warmup 40m -timeout 60m'`).
Проверяются обучение, replay, версия плана, подтверждение, перенос и байты GET;
затем SPPR пересоздаётся с `PLANNER_TRAIN_COMMAND=false`: новая попытка rejected,
а план продолжает использовать прежнюю модель. Команда обучения восстанавливается
пересозданием SPPR; настройки Planner восстанавливаются в defer, ошибка
восстановления завершает smoke ненулевым кодом. Перезапуск управляется флагами
`-failure-restart-command` / `-restore-restart-command`; они нужны для отличного
от Compose стенда. Bucket и журналы остаются для проверки. Docker smoke и
соблюдение ресурсной квоты требуют отдельного запуска на стенде.

## Разработка и тесты

```bash
make test-sppr
SPPR_TEST_PG_DSN='postgres://user:password@localhost:5436/test_db?sslmode=disable' make test-sppr-integration
SPPR_TEST_PG_DSN='postgres://user:password@localhost:5436/test_db?sslmode=disable' SPPR_TEST_KAFKA_BOOTSTRAP=localhost:9092 make test-sppr-integration
cd services/sppr
go generate ./pkg/mocks
go vet ./...
# Offline cross-language round trip, without PostgreSQL/Kafka:
SPPR_TEST_PYTHON="$PWD/ml/.venv/bin/python" go test ./internal/service/modelmanager -run TestPythonTrainingRoundTrip -v
```

Для `go generate ./pkg/mocks` нужен `minimock` v3.4.7 в `PATH`; исходные
директивы также закрепляют версию зависимостью сервиса.

`internal/handler/access`, `internal/client/identity`, `internal/handler/admin`
проверяются table-driven minimock-тестами без внешних сервисов: 401/403/503,
metadata/claims, cookie login/logout, маршруты/формы, отказ/зависимости, Origin и
HTML escaping. `internal/repository/adminform/repository_integration_test.go`
проверяет concurrent retry, сохранённый отказ, 409 и rollback эффекта; без
`SPPR_TEST_PG_DSN` пропускается.

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

Полнота истории определяется heartbeat и общей W по наблюдаемым потокам.
Буфер sarama в Gateway находится в памяти: переполнение/ошибка доставки тратят
sequence, Ingestor отмечает gap по следующему событию/heartbeat; при crash поток
закрывается по timeout с gap неизвестного размера. После потери номеров граница
этого потока остаётся временем последнего события; если все события потеряны,
она неизвестна. Поздние сообщения закрытого потока не открывают его снова.
Старт нового Gateway создаёт новые stream_id. Протокол не обнаружит процесс,
от которого не было доставлено ни одного сообщения; эксплуатация требует
согласованных часов Gateway/SPPR. Полнота обучения не выводится из MAX(timestamp).
Feature Builder, EWMA, подготовка меток и обучение на истории стенда используют
ту же `Coverage`; неизвестная W не превращает отсутствие событий в нулевой спрос.
Минутного агрегата недостаточно для произвольной границы T; точные события сохраняются.
Реестр версий моделей хранится в `model_versions`, артефакты — в постоянном томе;
ревизии планов хранятся в `plans`.

Нет retention/partitioning истории, DLQ, восстановления пропусков, SASL/TLS
конфигурации Kafka. Запуск рассчитан на один
экземпляр во внутренней development сети: несколько исполнителей на одной БД не
поддерживаются, кроме защиты `SKIP LOCKED` при взятии. Квота 2 ядра / 2 ГиБ для
всего runtime, включая обучение внутри SPPR, подтверждается измерениями этапа 11.

Очистка работает только при `MIGRATION_ENABLED=true`: выключенный процесс или
недоступный Storage оставляет копии занимающими место. Качество прогноза на
трассах и синтетике — [отчёт этапа 8](../../docs/ml-report.md). Коэффициенты
`c`, `v`, `P`, Δmin — заглушки до измерений этапа 10. Каталог для признаков
содержит только текущие версии и старые версии в HOT, поэтому перезаписи ключа,
чьи старые версии лежат в COLD, Feature Builder не видит. Новая ревизия каждые
`PLANNER_INTERVAL` делает прежнюю неподтверждаемой: план нужно подтвердить до
следующего расчёта. Длительность одного потока копирования не ограничена отдельным таймаутом.

## Связанные документы

- [Обучение прогноза](ml/README.md) — Python-пакет `sppr_ml`, notebook этапа 8, результаты.

- [Шаг 6](../../docs/sppr-step6.md) — учебная строка, генератор, признаки, EWMA и ограничения приёмки.

- [Онбординг](ONBOARDING.md) — порядок чтения, алгоритм и ручная проверка.
- [Спека Kafka](../../docs/kafka-spec.md) — контракт, доставка и отказоустойчивость.
- [Gateway](../gateway/README.md) — HTTP PUT/GET.
- [Producer, PR #1](https://github.com/WithSoull/s3-sppr/pull/1) — отдельная публикация обращений.
- [Users](../users/README.md) — образец слоёв и DI.
- [Metadata](../metadata/README.md) — существующие Kafka producers; протокол `PlacementService` и копии для очистки.
- [Storage](../storage/README.md) — `StoreObject` с заданным `blob_id`.
- [Правила разработки](../../AGENTS.md).
