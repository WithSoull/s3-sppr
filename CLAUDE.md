# CLAUDE.md — OpenS3-ReBAC

> Context for AI assistants. Contains only what can't be derived from reading the code.

## Что есть в текущем checkout

## What this project is

**OpenS3-ReBAC** — distributed S3-compatible object storage with **ReBAC** (Relationship-Based Access Control) authorization. Team learning project (4 people).

Repo: https://github.com/alesplll/opens3-rebac

- protobuf source of truth: `shared/api/<service>/v1/*.proto`;
- generated Go: `shared/pkg/go`;
- generated Python: `shared/pkg/py`;
- общие библиотеки: `shared/pkg/go-kit`, `shared/pkg/py_kit`,
  `shared/pkg/rust-kit`;
- Go-сервисы: `services/{auth,users,metadata,storage,gateway,sppr}`;
- Python AuthZ: `services/authz`;
- Rust Quota: `services/quota`.

## Services

| Service | Lang | Port | Owner | Status |
|---|---|---|---|---|
| **Gateway** | Go | `:8080` HTTP | Max | 🟡 Local PUT/GET/DELETE with Range and HOT/COLD routing; Auth/AuthZ/Quota pending |
| **SPPR** | Go | `:8086` health + migration/planner APIs + /admin (127.0.0.1) | — | Kafka Ingestor; HOT/COLD migration executor (`MIGRATION_ENABLED`); Planner on EWMA/LightGBM (`PLANNER_ENABLED`); profile `sppr` |
| **Auth** | Go | `:50050` gRPC | — | ✅ Complete |
| **AuthZ (ReBAC)** | Python | `:50051` gRPC | Alexa | ✅ Complete |
| **Metadata** | Go | `:50052` gRPC | Anya | ✅ Complete; HOT/COLD placements + PlacementService |
| **Storage** | Go | `:50053` gRPC (`storage-cold`), host `:50056` (`storage-hot`) | Ilya | ✅ Complete; two instances on separate volumes |
| **Users** | Go | `:50054` gRPC | — | ✅ Complete |
| **Quota** | Rust | `:50055` gRPC | — | ✅ Complete |

Proto source: `shared/api/`. Generated stubs: `shared/pkg/go/`, `shared/pkg/py/`.

`Login` возвращает refresh token. Access token выдаёт отдельный
`GetAccessToken`. `GetRefreshToken` выпускает новый token, но пока не отзывает
старый. JWT содержит строковый UUID в claim `user_id` и `token_type`; `sub` не
является текущим источником ID. Redis используется для login-attempt counters.

SPPR `/admin` and `/api/{planner,migration}` use Auth then AuthZ when
`ADMIN_AUTH_ENABLED` is on (defaults to `PLANNER_ENABLED`). ValidateToken takes
Empty and metadata `authorization: Bearer <access token>`; trust `user_id` only
following success and require `token_type=access`. AuthZ ADMIN resource is
`system:placement`: current servicer accepts it and store infers Resource.
`make up-sppr-planner` starts identity services too; `make seed-planner-admin`
prints local credentials for `http://localhost:8086/admin`. Login exchanges
refresh for access, cookie is HttpOnly/SameSite=Strict; expiry requires login.
Runbook: [SPPR access](services/sppr/README.md#доступ-и-html-панель).


Stage 9 is wired in `planner_provider.go`: ModelManager restores the active
journal entry, optionally bootstraps an empty journal, and swaps immutable
predictors between cycles. Registry files are on the `sppr-model-registry`
Compose volume; bootstrap remains a read-only host mount. The image includes
Python 3.12 and libgomp. Training uses real Coverage and the same minute feature
path; it must not borrow the planner's temporary completeness assumption.
Status additions live under `model_lifecycle`; `/admin/models` shows attempts.
Configuration and the restart-based smoke are in the
[SPPR runbook](services/sppr/README.md#выбор-предиктора-и-каталог-модели).

## Architecture

```
Client (HTTP / S3 API)
        │
        ▼
   Gateway :8080       ← single entry point
  /   |    |    \
Auth AuthZ Meta Storage
  │    │    │      │
  │  Neo4j PostgreSQL filesystem
  │  Redis  Kafka
Users :50054 ── PostgreSQL
Quota :50055 ── Redis ── Kafka
Kafka object-read, object-written → SPPR :8086 (health, /api/migration, /api/planner) → PostgreSQL SPPR
SPPR planner → Metadata catalogue (ListPlacements, ListStorageClasses) → plan → administrator confirm → executor
SPPR migration executor → Metadata PlacementService + Storage HOT/COLD (direct, not via Gateway)
```

Gateway → Auth and Gateway → AuthZ are not implemented yet: Gateway currently
serves local PUT/GET and calls only Metadata and Storage. With
`ACCESS_EVENTS_ENABLED=true` it also publishes successful GET/PUT to Kafka
without waiting for it; see [Kafka spec](docs/kafka-spec.md).

---

## Key domain concepts

### S3 → ReBAC mapping

| S3 operation | action | resource |
|---|---|---|
| `GetObject` | `read` | `object:{bucket}/{key}` |
| `PutObject` | `write` | `object:{bucket}` ← **bucket**, not object (doesn't exist yet) |
| `DeleteObject` | `delete` | `object:{bucket}/{key}` |
| `ListBucket` | `read` | `bucket:{bucket}` |
| `CreateBucket` | — | no check — any user can create |
| `DeleteBucket` | `delete` | `bucket:{bucket}` |

### Kafka topics

| Topic | Producer | Consumer |
|---|---|---|
| `object-stored` | Storage | Metadata |
| `object-deleted` | Metadata | Storage, AuthZ |
| `bucket-deleted` | Metadata | AuthZ |
| `auth-changes` | AuthZ | AuthZ cache_invalidator |
| `auth-audit` | AuthZ | — (log sink) |
| `object-read` | Gateway (успешный GET) | SPPR |
| `object-written` | Gateway (успешный PUT) | SPPR |

### HOT/COLD placement

Metadata is the source of truth for where each version is stored:
`version_placements` (class, blob_id, generation). Before writing, Gateway asks
`ChooseWriteClass`: a new key goes to COLD; an overwrite of a HOT key with known
size stays in HOT if it fits (no reservation). Gateway reads and writes through
`storage-hot` or `storage-cold` by that class; their addresses are Gateway config.
`PlacementService` (same port as MetadataService) lets the migration executor
reserve a copy, switch the placement by CAS and record cleanup. The executor runs
in SPPR ([README](services/sppr/README.md#перенос-версии)); its cleanup loop deletes
due source, aborted target and orphan blobs, then confirms deletion to Metadata.
Migration pause does not stop cleanup; both run only with `MIGRATION_ENABLED=true`. See
[Metadata README](services/metadata/README.md#перенос-версии-между-классами).

Публичные relations: `MEMBER_OF`, `HAS_PERMISSION`, `PARENT_OF`. Для владельца
используйте `HAS_PERMISSION` с уровнем `ADMIN`; `OWNER_OF` — только legacy-чтение
в store и не принимается публичным enum.

## Service boundaries

SPPR самостоятельно читает Kafka и сохраняет историю, gaps и минутный спрос.
Полнота этапа 5: Gateway публикует heartbeat каждого топика под mutex потока,
включая closing после HTTP/buffer drain. SPPR различает события по Kafka header,
атомарно сохраняет heartbeat/progress/gaps и закрывает потерянные потоки фоновой
проверкой с управляемыми часами. Lost-check допускает атомарное возобновление того же stream_id (graceful closing — нет); только непрерывность удаляет текущий lost gap, см. восстановление в SPPR README.
`repository/history.Coverage` — источник общей W;
Planner считает на W, исключает будущие события до SQL-группировки и останавливает
план при чрезмерном lag. Отсутствие чтений и неполная история различаются в
status/admin. Протокол: `docs/history-completeness-spec.md`; конфигурация,
восстановление и disruptive `smoke-history` — в SPPR README.
Runbook — [README](services/sppr/README.md), маршрут чтения —
[онбординг](services/sppr/ONBOARDING.md). Gateway producer из отдельного
[PR #1](https://github.com/WithSoull/s3-sppr/pull/1) уже есть в checkout;
для запуска и тестов Ingestor он не нужен. Контракт —
`shared/api/events/v1/access.proto`, решения — [спека Kafka](docs/kafka-spec.md).
Исторически оба PR зависели от общей ветки `feat/access-events-contract`.

| Service | Does NOT |
|---|---|
| **AuthZ** | authenticate users, store metadata, handle bytes, know about HTTP |
| **Metadata** | store bytes, check permissions, know about S3 API |
| **Storage** | check permissions, store metadata, know about object keys |
| **Quota** | enforce auth, store metadata, know about blobs |
| **Auth** | authorize (that's AuthZ), manage user profiles |
| **Users** | issue tokens, know about S3, check permissions |
| **Gateway** | store data, make authorization decisions, know about graph structure |

1. [AGENTS.md](AGENTS.md): стиль Go/Python, minimock, документация и scope изменений.
2. [Карта документации](docs/README.md): README нужного сервиса и связанные планы.
3. Wire fields в `shared/api`, затем handler/service/repository: комментарии proto
   тоже могут быть устаревшими. Наличие комментария о Kafka consumer не доказывает,
   что consumer подключён в runtime.
4. `docker-compose.yml`, сервисный `.env` и config parser: отличайте defaults кода,
   значения development и host overrides.

## Пакеты и границы API

| Phase | Status | What |
|---|---|---|
| **Phase 0** | ✅ Done | Contracts, Docker Compose, shared infrastructure |
| **Phase 1** | 🔄 In Progress | MVP: PutObject + GetObject end-to-end |
| **Phase 2** | ⏳ | CreateBucket, DeleteBucket, DeleteObject, Kafka wiring, versioning |
| **Phase 3** | ⏳ | Multipart upload, object sharing, full S3 compatibility |
| **Phase 4** | ⏳ | Audit, E2E tests |

---

## Working guidelines

### Commits
- One logical change per commit. No `Co-Authored-By` lines.
- Format: `type(scope): short description` — e.g. `feat(metadata): add ListObjects handler`.

### Tests
- Every feature or bug fix must include tests or an explicit offer to add them.
- Go: table-driven with `testify`, mocks via `minimock`.
- Python: `pytest`, fixtures in `conftest.py`.
- Rust: `#[cfg(test)]` for unit, `tests/` for integration.
- Integration tests requiring external infra (Neo4j, Redis) must be skippable without it.

### Documentation hygiene
When a gRPC contract changes or a new service is added, update:
- `shared/api/` proto file
- `README.md` (service section + architecture diagram)
- `CLAUDE.md` (services table, domain concepts if affected)
- The service's own `README.md`

### Follow existing patterns
Before implementing anything foundational (handler, service, repository, config, migrations, tests), **look at how it's done in the project first**.

The canonical reference is **`services/users/`** — it covers handler structure, service layer with interface + mock, PostgreSQL repository, domain models, env config, table-driven tests, and SQL migration layout.

### Branches
- Format: `type/scope-description` — e.g. `feat/authz-cache-invalidation`, `fix/users-uuid-parsing`.

### Issues
- Title: specific and actionable — what's broken or what needs to be done.
- Body: `## What` (2-3 sentences) + `## Acceptance criteria` (checklist).
- For bugs: include reproduction steps in the What section.

### Pull requests
- Title format matches commits: `type(scope): short description`.
- Link to issue via `closes #N` in PR body when applicable.
- Descriptions: concise, no filler.

### GitHub (gh CLI)
- Before any write action (review, comment, close, merge) — show draft and wait for confirmation.
- Before reviewing a PR — always run `gh pr diff` + `gh pr view` to understand context.
- Don't guess the intent of changes — ask if unclear.

### Code style
- No comments explaining WHAT — only WHY when non-obvious.
- No speculative abstractions. Implement only what the task requires.
- Error handling only at system boundaries (user input, external calls).
