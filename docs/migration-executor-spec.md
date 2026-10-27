# ТЗ: исполнитель переносов HOT/COLD (этап 4.3)

Задание агенту. Этап 4, часть 3 плана до предзащиты (заметка «План до предзащиты»):
исполнитель принимает подтверждённые действия переноса, безопасно копирует
версию между классами, переключает расположение и очищает старые blobs.
Требования диплома: F-08–F-10 (перенос без потери данных, ограничения ресурсов,
журнал). Модель и автоматический выбор объектов — не в этом этапе.

## Общее задание

Выполни этап по частям ниже. Сначала прочитай существующий код и сохрани готовое:
протокол переноса уже есть в Metadata `PlacementService` (PR #4), Storage
принимает `blob_id` (PR #5), ручной перенос показан в
`services/gateway/cmd/smoke/classes.go`. Не переписывай их без конкретной причины.
Стиль — как в `services/users` и существующем `services/sppr`: service provider,
go-kit клиенты, слои handler/service/repository, minimock, table-driven tests.
Если требования ниже противоречат коду или друг другу — покажи противоречие до
реализации спорного места. Каждая часть — отдельный PR с summary по форме плана
(что работает, маршрут чтения кода, пример, команды и фактический вывод, что не
сделано, что готово для следующего этапа). Следующую часть не начинай, пока
автор не принял предыдущую.

## Принятые решения

| Вопрос | Решение |
|---|---|
| Где живёт исполнитель | В `services/sppr`: свой компонент, таблицы в PostgreSQL SPPR, воркеры в процессе SPPR, включаются `MIGRATION_ENABLED` |
| Как приходят команды | HTTP JSON API на HTTP-сервере SPPR (:8086). Этап 7 (Planner, админ-панель) вызывает те же сервисные методы и ручки |
| Единица работы | Действие плана с `depends_on`; ошибка предшественника отменяет зависимые |
| Бюджет копирования | Задаёт администратор через API; учитываются все попытки, включая неуспешные; действие сверх бюджета ждёт |
| Закрепление | По ключу bucket/key, смысл «не переносить»; перепроверяется перед стартом действия |
| Минимальный интервал Δmin | Настройка администратора; перепроверяется перед стартом действия |
| Параллельность | Пул воркеров `MIGRATION_WORKERS`, по умолчанию 2 |
| DELETE объекта | Metadata превращает расположения версий удалённого объекта в копии для очистки; их удаляет тот же цикл очистки исполнителя |
| Спрос | Исполнитель читает и пишет Storage напрямую, не через Gateway, поэтому технические копии не попадают в `object-read` |

## Порядок частей

1. **Metadata: очистка blobs удалённых объектов** — маленький независимый PR.
2. **SPPR: действия, воркеры и восстановление** — ядро исполнителя.
3. **SPPR: цикл очистки, проверки сбоев, smoke и документация.**

Агент может предложить другую нарезку, если покажет причину до начала.

---

## Часть 1. Metadata: очистка blobs удалённых объектов

Сейчас `DeleteObjectMeta` каскадно удаляет версии и их `version_placements`.
Blobs остаются в Storage и выпадают из занятости классов (ограничение в README
Metadata).

Сделать:

- В той же транзакции, что и удаление объекта, для каждой версии с расположением
  вставить строку `placement_copies` новой роли (например, `orphan`) в состоянии
  `cleanup_pending` с `storage_class` и `blob_id` расположения, `size_bytes` версии и
  `cleanup_after = now() + METADATA_DELETE_CLEANUP_DELAY`.
- Схема: сейчас `migration_id NOT NULL` и `UNIQUE (migration_id, role)`. Разрешить
  отсутствие `migration_id` только для новой роли (миграция up/down, CHECK).
  Вариант с фиктивным UUID не использовать.
- `METADATA_DELETE_CLEANUP_DELAY` — env Metadata, по умолчанию 10m. Должна быть
  больше `GATEWAY_MAX_READ_DURATION`: GET, начатый до DELETE, обязан дочитать файл.
  Compose задаёт обе из общих переменных.
- `ListDueCleanups` и `CompleteCleanup` работают с новой ролью без изменений API.
  `PlacementCopyInfo.role` документирует новое значение в `placement.proto`.
- Зарезервированная целевая копия (`reserved`) удаляемой версии остаётся как
  есть: исполнитель получит ABORTED/NOT_FOUND на `SwitchPlacement` и вызовет
  `AbortCopy`. Проверить, что это так.
- Событие `object-deleted` не менять.

Тесты: integration — удаление объекта с версиями в HOT и COLD даёт копии
`cleanup_pending` по одной на версию, занятость классов не уменьшается до
`CompleteCleanup(deleted=true)`; удаление во время переноса оставляет и копию
`reserved`, и копии удалённых версий. Unit — ошибки репозитория откатывают удаление.

Готово: после DELETE место в классе освобождается только после очистки blob, и
blob виден в `ListDueCleanups` после задержки.

---

## Часть 2. SPPR: действия, воркеры, восстановление

### Модель действия

```text
action_id            UUID от вызывающего; ключ идемпотентности; используется как migration_id в Metadata
plan_id              UUID плана (на этапе 4.3 — произвольный)
version_id           UUID версии
expected_generation  поколение расположения, на котором строился план
source_class         HOT | COLD
target_class         HOT | COLD, не равен source_class
size_bytes           размер версии в плане
depends_on           список action_id того же плана
```

Состояния:

| Статус | Значение |
|---|---|
| `pending` | ждёт; `blocked_by`: `dependencies`, `budget`, `paused` или пусто |
| `running` | взято воркером; `step`: `reserving`, `copying`, `switching` |
| `succeeded` | расположение переключено |
| `failed` | попытка была и не удалась; `reason` обязателен |
| `cancelled` | не выполнялось по решению; `reason`: `user`, `pinned`, `min_interval`, `dependency_failed` |

Коды `failed.reason`: `stale` (CAS/версия не текущая), `no_capacity`,
`verify_mismatch` (размер или MD5), `source_missing`, `upstream_unavailable`
(исчерпаны попытки), `internal`. Каждый переход пишется в журнал событий действия
(время, из какого в какой статус/шаг, байты, ошибка).

### Хранение (миграция SPPR)

- `migration_actions` — поля действия, статус, шаг, причина, число попыток,
  `bytes_charged`, времена создания/старта/завершения.
- `migration_action_deps` — рёбра `depends_on`.
- `migration_action_events` — журнал.
- `migration_budget_ledger` — списания бюджета (action_id, байты, время).
- `migration_settings` — одна строка: бюджет (байт за окно, длительность окна),
  Δmin, флаг паузы. Значения по умолчанию из env при первом старте.
- `migration_pins` — bucket, key, причина, автор, время; уникален по bucket/key.

### Обработка действия воркером

1. Взять действие одним запросом с `FOR UPDATE SKIP LOCKED`: `pending`, все
   зависимости `succeeded`, не пауза, хватает остатка бюджета. В той же транзакции
   списать `size_bytes` в ledger и перевести в `running/reserving`.
2. `GetPlacement(version_id)`: если ключ закреплён → `cancelled/pinned`; если
   `generation > 1` и `now - updated_at < Δmin` → `cancelled/min_interval`.
   Списание бюджета при этих отменах вернуть (байты не передавались).
3. `ReserveCopy(migration_id = action_id, …)`. ABORTED → `failed/stale`,
   RESOURCE_EXHAUSTED → `failed/no_capacity`; списание вернуть.
4. `copying`: потоком прочитать blob из Storage класса источника и записать в
   Storage цели с `blob_id` из резервирования, считая MD5 и байты. Списание бюджета
   скорректировать до фактически переданных байт.
5. Сверить размер с `size_bytes` версии и MD5 источника с ответом цели. Не совпало →
   `AbortCopy` → `failed/verify_mismatch`.
6. `switching`: `SwitchPlacement` с `source_cleanup_delay = MIGRATION_SOURCE_CLEANUP_DELAY`.
   ABORTED → `AbortCopy` → `failed/stale`.
7. `succeeded`.

Ошибки Unavailable/DeadlineExceeded Metadata или Storage повторяются с backoff до
`MIGRATION_MAX_ATTEMPTS`; затем `AbortCopy` (если копия зарезервирована) и
`failed/upstream_unavailable`. Байты всех попыток остаются списанными.
NOT_FOUND blob источника → `AbortCopy` → `failed/source_missing`.

При переходе в `failed` все ещё не начатые действия, зависящие от него
транзитивно, становятся `cancelled/dependency_failed`.

### Восстановление после рестарта

На старте каждое `running` действие продолжается с шага по Metadata
`ListMigrationCopies(action_id)`:

- копий нет → снова `ReserveCopy` (идемпотентно);
- `reserved` → скопировать заново (повторная запись с тем же `blob_id` заменяет
  файл), сверить, переключить;
- `promoted` + источник → `succeeded`;
- целевая копия в `cleanup_pending` (была отмена) → `failed` с прежней причиной из
  журнала или `internal`.

Повторный `SubmitActions` с тем же `action_id` и теми же полями — no-op; с другими
полями — 409.

### HTTP API (JSON, префикс `/api/migration`)

| Метод | Путь | Назначение |
|---|---|---|
| POST | `/actions` | `{plan_id, actions:[…]}` → 202 и список принятых/уже существующих. Проверить: UUID, классы, `depends_on` в том же запросе или уже принятые этого плана, без циклов |
| GET | `/actions?plan_id=&status=&page_size=&page_token=` | список с пагинацией по `action_id` |
| GET | `/actions/{id}` | действие, зависимости, журнал |
| POST | `/actions/cancel` | `{plan_id}` или `{action_ids}`; отменяет только `pending`, возвращает, что отменено |
| POST | `/pause`, `/resume` | новые действия не берутся; начатые доделываются; очистка продолжается |
| GET / PUT | `/budget` | `{bytes, window_seconds}`; GET также `spent`, `remaining` за текущее скользящее окно |
| GET / PUT | `/settings` | `{min_interval_seconds}` |
| GET / POST / DELETE | `/pins` | закрепления `{bucket, key, reason, author}` |

Аутентификации пока нет (этап 7): `author` — свободный текст. Ручки только на
адресе SPPR внутри стенда; в Compose порт публиковать на `127.0.0.1`.
Сервисный слой — отдельный интерфейс (`service.MigrationService`), HTTP — тонкий
handler, чтобы Planner этапа 7 вызывал методы без HTTP.

### Конфигурация

| Переменная | По умолчанию | Смысл |
|---|---|---|
| `MIGRATION_ENABLED` | `false` | запускать воркеры и цикл очистки; API отвечает и при `false` |
| `MIGRATION_WORKERS` | `2` | параллельные действия |
| `MIGRATION_MAX_ATTEMPTS` | `5` | попытки при временных ошибках |
| `MIGRATION_RETRY_BACKOFF` | `1s` | начальная пауза, экспоненциально |
| `MIGRATION_SOURCE_CLEANUP_DELAY` | `10m` | `cleanup_after` источника; должна быть больше `GATEWAY_MAX_READ_DURATION`, иначе старт с ошибкой |
| `GATEWAY_MAX_READ_DURATION` | `5m` | тот же параметр, что в Gateway; Compose берёт одну переменную |
| `MIGRATION_BUDGET_BYTES`, `MIGRATION_BUDGET_WINDOW` | `1GiB`, `1h` | начальное значение бюджета |
| `MIGRATION_MIN_INTERVAL` | `1h` | начальное значение Δmin |
| `METADATA_GRPC_ADDR`, `STORAGE_HOT_GRPC_ADDR`, `STORAGE_COLD_GRPC_ADDR` | как в Gateway | клиенты |

### Тесты части 2

- Unit (minimock): каждый исход шагов 2–6, возврат и корректировка бюджета,
  каскад `dependency_failed`, идемпотентность и 409 при Submit, валидация API.
- Repository integration на PostgreSQL SPPR (skippable без env): `SKIP LOCKED` не
  отдаёт одно действие двум воркерам; зависимое не берётся до `succeeded`
  предшественника; бюджет не превышается при конкурентном взятии.

Готово: действие проходит путь COLD → HOT на стенде через API; рестарт процесса
посреди `copying` завершает действие без второй копии.

---

## Часть 3. Очистка, сбои, smoke, документация

### Цикл очистки

Каждые `MIGRATION_CLEANUP_INTERVAL` (по умолчанию 30s) при `MIGRATION_ENABLED`:
`ListDueCleanups(limit)` → `DeleteObject(blob_id)` в Storage класса копии →
`CompleteCleanup(deleted=true)`; ошибка Storage → `CompleteCleanup(deleted=false,
error)`. Работает и для источников переноса, и для отменённых целей, и для
копий удалённых объектов из части 1. Пауза переносов очистку не останавливает.

### Проверки сбоев

Integration на стенде (`make up-gateway` + SPPR; skippable без env). Сбои
внедряются обёрткой клиента Storage/Metadata в тесте, рестарт — остановкой
исполнителя и запуском нового экземпляра на той же БД.

| Сценарий | Ожидание |
|---|---|
| Повреждённая копия (обёртка меняет байт) | `failed/verify_mismatch`, расположение прежнее, цель уходит в очистку |
| Цель недоступна | повторы, затем `failed/upstream_unavailable`, `AbortCopy`, бюджет списан за переданные байты |
| Нет места в HOT | `failed/no_capacity`, бюджет возвращён, зависимые `cancelled` |
| Перезапись объекта во время копирования | `failed/stale`, новая версия не затронута |
| Удаление объекта во время копирования | `failed/stale` или `source_missing`, всё уходит в очистку |
| Конкурентное чтение во время переключения | GET через Gateway всё время возвращает верные байты |
| Рестарт на каждом шаге (`reserving`, `copying`, `switching`, после switch) | одно переключение, одна целевая копия, `succeeded` |
| Ошибка удаления источника | `cleanup_failed`, место занято, следующая попытка очищает |
| Закрепление после Submit | `cancelled/pinned`, бюджет не списан |
| Повтор раньше Δmin | `cancelled/min_interval` |
| Бюджет исчерпан | действие ждёт в `pending/budget` и стартует после окна |

### Smoke и демонстрация

`make smoke-migration` (новая команда SPPR): PUT через Gateway → действие COLD → HOT
через API → ожидание `succeeded` → GET тех же байт → действие HOT → COLD →
ожидание очистки источника (в smoke короткая задержка через env) → источник
удалён из Storage, занятость HOT вернулась к исходной. Ручной перенос в
`services/gateway/cmd/smoke/classes.go` заменить вызовом исполнителя или оставить
с пометкой, что это проверка протокола Metadata, — выбрать и обосновать.

Для автора — команды curl в README SPPR, чтобы руками провести один объект
COLD → HOT → COLD и посмотреть журнал действия.

### Наблюдаемость

Метрики: `sppr_migration_actions_total{outcome,reason}`,
`sppr_migration_bytes_copied_total{target_class}`,
`sppr_migration_budget_remaining_bytes`, `sppr_migration_cleanups_total{outcome}`,
`sppr_migration_duration_seconds`. Логи переходов с `action_id` и `version_id`;
bucket/key в labels не класть.

### Документация

README SPPR (разделы по AGENTS.md), README Metadata (роль копии для удалённых
объектов, `METADATA_DELETE_CLEANUP_DELAY`), корневые README/CLAUDE.md/AGENTS.md,
`docker-compose.yml` (env SPPR и Metadata). Обновить ограничения: что из этапа 7
(Planner, HTML, авторизация) пока отсутствует.

## Что понять автору (из плана)

- Зачем копия проверяется до CAS.
- Почему источник нельзя сразу удалять и как с этим связан `GATEWAY_MAX_READ_DURATION`.
- Что происходит после каждого возможного сбоя: какое состояние остаётся в
  Metadata, в БД SPPR и на дисках.

## Не делать

- Planner, прогноз, HTML-панель и авторизацию (этапы 6–7).
- Несколько исполнителей или распределённую блокировку сверх `SKIP LOCKED`.
- Delete marker и S3-версионирование.
- Отдельный сервис `services/migrator` (решение изменено: исполнитель в SPPR).
