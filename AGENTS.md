# Repository Guidelines

## Project Structure & Module Organization

This repository is a monorepo for several gRPC services and a small HTTP Gateway. Main code lives under `services/`: Go services in `services/auth`, `services/users`, `services/metadata`, `services/storage`, and `services/gateway`; the Python ReBAC service in `services/authz`; and the Rust quota service in `services/quota`. Shared protobuf sources are in `shared/api`, generated clients/stubs live in `shared/pkg/go` and `shared/pkg/py`, infrastructure lives in `infra/`, and cross-service docs are in `docs/`.

Go services follow the same layout: `cmd/server` for server entrypoints and `internal/` for app, config, handlers, services, and repositories. Additional maintenance binaries may live under `cmd/<name>`. AuthZ keeps runtime code in `entrypoints/` and `internal/`, with tests in `services/authz/tests/{unit,integration}`. Quota keeps Rust code in `services/quota/src`.

`services/sppr` is the Go decision-support foundation: Kafka Ingestor, the
HOT/COLD migration executor (`/api/migration`, workers behind `MIGRATION_ENABLED`),
the Planner (`/api/planner`, behind `PLANNER_ENABLED`, which needs the executor) and
the Feature Builder/EWMA used both by the planner and by an offline dataset pipeline. `make demo-sppr-data` prints
the hand-checkable example; `make dataset-sppr` generates a small seven-day
dataset without external infrastructure. The dataset CLI also accepts external
`-catalog`/`-events` with explicit coverage/split bounds, `-format csv.gz` and
`-train-every`. `cmd/trace-convert` defaults to Meta CDN CSV/zstd and supports IBM COS
(`-source ibm-cos`, streaming tar gzip/Unix compress or text). COS needs an explicit
`-origin`, one bucket per trace, and inferred-success opt-in just like Meta. Preserve
the source manifest, deterministic key sampling and row policies. Docker Registry
(`-source docker -zone <zone>`, optional filename `-from`/`-to` days) streams tar.gz
JSON arrays and externally sorts selected reads using `/usr/bin/sort`; one bucket
per zone, digest keys, full GET 200 size, GET 200/206 demand. Data license is
not stated. See `docs/traces.md`. See the service README for profiles.
Use its service provider and shared go-kit clients. Preserve commit-before-ack,
stream deduplication and atomic event/progress/gap/aggregate updates.
Stage 5 uses same-stream `StreamHeartbeat` barriers (Kafka `event-type: heartbeat`),
periodic and closing heartbeats under the event mutex; HTTP never waits for Kafka.
Shutdown drains HTTP, confirms ordinary barriers, confirms closing barriers, then
closes sarama; consume both results channels. History Coverage owns W across
observed open/closed/lost streams; never infer it from event traffic or now.
Lost-check closure can resume atomically on a new non-regressing message; resolve only its marked lost gap on contiguity, preserve graceful closure and replay idempotency (see the SPPR recovery runbook).
Planner runs at W, gates excessive lag and reads only events strictly before W
before grouping by minute. Keep gaps conservative and heartbeat replay atomic.
See `docs/history-completeness-spec.md` and the SPPR README for env and recovery.
`make smoke-history-local` disrupts Kafka/Gateway: use a dedicated stand. Mark events
that a retry cannot fix with `model.ErrRejected` so they are committed; return
infrastructure errors so the go-kit `Retry` middleware holds the partition. Its SQL
migrations use migrate up/down files. See `services/sppr/README.md` for the event
contract and unfinished roadmap steps. The executor drives every action from the
Metadata copy state of its `action_id` (= `migration_id`), so a claim and a resume
after restart share one path; record the failure reason (`aborting`) before
`AbortCopy` and settle budget charges in the same transaction as the transition.
Cleanup runs independently of migration pause and handles due copies of every
role, including `orphan` without a migration ID. Only successful Storage deletion
may release Metadata's class charge; retry deletion safely if confirmation fails.
A dependent action starts only after its predecessor succeeded and Metadata reports
its source copy deleted (`source_cleaned_at`, set by the cleanup loop): before
that the source still holds the space the dependent needs. The planner's greedy
`planner.Build` is pure and tested against exhaustive search; keep explanations
computed from the same values as the plan. Administrator commands are applied
once per `command_id` and journaled, refused ones included.
Administrator routes use Auth.ValidateToken (Bearer metadata) then AuthZ ADMIN
on `system:placement`; only validated JWT `user_id` is an initiator. Access
defaults on with Planner; explicitly disabling it is for local tests and logs a
warning. Panel templates are embedded; preserve Origin/Referer checks, HttpOnly
SameSite cookies and command_id replay. Executor settings/pins from panel use
`admin_form_commands` atomically with their effects. See the service README for
login, seed and smoke instructions.
The logical-time experiment tools (`internal/experiment/sim`, `cmd/simulate`,
`cmd/experiment`, `experiments/*.json`) call the production `features.Build` and
`planner.Build`; never add a separate placement heuristic. Demand/features and
forecasts are shared per (scenario, seed), placement/queues are isolated per
policy/budget. Preserve causal labels, measured training activation delays,
capacity including temporary copies, FIFO reads, per-cycle budget and checkpoint
hash verification. `make sim-smoke`, `make experiment CONFIG=...`, and
`make experiment-report` need the ML venv but no services. New result directories
are mandatory; `-resume` requires the same binary/config/revision. `mode=stand` runs serial disposable Compose projects, with loopback ephemeral
ports and `down -v` before/after each policy. Preserve the predetermined open-loop
arrival schedule, FIFO queue and <=8 active full GETs; latency includes queueing
through the last byte, errors must account for every offered request. Compression
changes arrival time only, not service clocks. Keep 2 CPU / 2 GiB SPPR limits,
inspect verification, docker stats and status/journal artifacts. Overhead uses a
fixed COLD placement, paused migrations, no confirmations, and paired
no-extension/no-training/training runs; Python training/export shares the SPPR
cgroup. This checkout has no Model Manager hot activation. Unknown stand
invariants/metrics must stay unknown; snapshots alone never prove zero violations.
`make stand-smoke`, `make stand-experiment`, `make overhead` require Docker;
never run against an existing project. ML/map/train-only inputs must exist before
the run and are hashed for resume. See the SPPR README for model assumptions and overrides.
Stage 11 measurement CLIs: `make forecast-quality` evaluates TEST only with
fixed params and train-only fitting; each seed dataset/cache is temporary and
only evaluate.json remains. Stage 8 must still never read TEST. `make bench-admin`
measures a running dedicated stand, journals no-op pause/resume commands and can
restart it only when explicitly given -restart-command. `make bench-plan` measures
production features/predictors/Build in memory; report its timing/heap/RSS scope
accurately. See the SPPR README before running heavy measurements.
Baseline predictors register through `PLANNER_PREDICTOR=age|recency|frequency`;
age/recency need `PLANNER_BASELINE_MAP` (one matching map, trained on train only).

Stage 9 uses `service/modelmanager`: a durable `model_versions` journal and a
persistent writable registry. Every planner cycle captures one predictor; do not
read the atomic manager again per object. Live `dataset.FromHistory` must use
`history.Coverage` directly for feature and label quality, never `assumeCaughtUp`.
Keep lifecycle status additive (`model_lifecycle`) for the parallel stage-5 work.
The trainer runs once at a time with a timeout, inherited fixed parameters and
`num_threads=2`; journal failed attempts and administrator retrain replay too.
`make smoke-ml-update-local` uses a dedicated empty-HOT stand, a 2m horizon and
40m of real reads because snapshots remain 15m. See the service README before
running it; it recreates SPPR to exercise a failing trainer and restores it.
`PLANNER_PREDICTOR=lgbm` (default) is the only mode with the model lifecycle;
`ewma|age|recency|frequency` pin one predictor for experiment comparison runs.
The ML training package lives in `services/sppr/ml` (`make ml-sppr-venv`,
`make test-sppr-ml`; see its README): never load the test split in stage 8, and
export models only in the "Схема файлов экспорта v1" of `docs/ml-spec.md`, which
the Go predictor validates. `make ml-sppr-traces` builds the trace datasets with
key share, snapshot step and periods fixed in `ml/scripts/trace_datasets.py`;
results are in `docs/ml-report.md`. Keep the report's `never_read` check: IBM COS
catalogues are inferred from the trace and leak future touches. Run `make test-sppr`; database/Kafka checks use `make test-sppr-integration` with
documented test environment variables. Real-service failure tests additionally
require `SPPR_TEST_MIGRATION_STAND=1`; their executor owns the run, so do not run a
competing SPPR executor/cleanup. See the SPPR README for endpoint overrides.

## Build, Test, and Development Commands

- `make up-services`: start local infrastructure and enabled services with Docker Compose.
- `make up-e2e`: start the dedicated local PostgreSQL container for Go integration tests.
- `make up-observability`: start Grafana, Prometheus, Jaeger, and related tooling.
- `make down` / `make down-volumes`: stop containers; the latter also removes persisted data.
- `make generate`: regenerate shared Go and Python protobuf bindings from `shared/api`.
- `go test ./...` from `services/auth`, `services/users`, `services/metadata`, or `services/storage`: run tests for one Go service module.
- `make test-gateway`: run the Gateway Go tests.
- `make smoke-gateway-local`: build/start the Gateway stack and run the real-service smoke (PUT/GET/HEAD, HOT/COLD routing, Range, DELETE); `make smoke-gateway` checks an already running stack.
- `make smoke-migration`: check COLD → HOT → COLD through the running SPPR API, source deletion and HOT occupancy; `SMOKE_MIGRATION_ARGS` forwards CLI flags.
- `make smoke-migration-local`: start the dedicated Compose demo with coordinated short read/cleanup limits and run the migration smoke.
- `make up-sppr-migration`: start SPPR with the migration executor enabled, plus Gateway, Metadata and both Storage instances.
- `make up-sppr-planner`: Planner, executor and identity stack (Auth/AuthZ/Users).
- `make seed-planner-admin`: provision the local test administrator; use its printed credentials at `http://localhost:8086/admin`.
- `make smoke-planner` / `make smoke-planner-local`: authenticated full-loop checks on a dedicated empty-HOT stand; local shortens read/cleanup limits. See the SPPR README for config, overrides and remaining recompute replay defect.
- `make up-gateway`: start Gateway, Metadata, the HOT and COLD Storage instances, and their local Compose dependencies.
- `make test-metadata-integration`: run Metadata integration tests sequentially (`-p 1`) against the dedicated e2e PostgreSQL profile; the suites reset a shared schema.
- `make test-metadata-integration-local`: start the dedicated e2e PostgreSQL container and then run Metadata integration tests.
- `python3 -m pytest tests/unit -v` from `services/authz`: run AuthZ unit tests.
- `python3 -m pytest tests/integration -v -m integration` from `services/authz`: run Neo4j-backed integration tests.
- `cargo test -p quota-service` from the repository root: run Quota tests.

## Coding Style & Naming Conventions

Gateway access events go through `service.AccessProducerService`
(`internal/service/producer/access`) on one shared sarama `AsyncProducer`; see
`docs/kafka-spec.md`. Never make HTTP wait for Kafka, publish only after a
successful operation and keep numbering, marshal and `Send` under the stream
mutex. Producer tests and startup must work without an Ingestor.

Use standard Go formatting (`gofmt`) and keep package names lowercase. Prefer the existing service layering (`handler`, `service`, `repository`) and place binaries only under `cmd/`. In Python, use 4-space indentation, `snake_case` for functions/modules, and keep gRPC service classes descriptive, for example `PermissionServiceServicer`.

Do not hand-edit generated protobuf files under `shared/pkg/`; regenerate them instead.

## Testing Guidelines

Keep Go tests close to the package they verify, using `*_test.go`. Current examples include handler and service tests under `services/users/internal/.../tests`. For AuthZ, use `tests/unit/test_*.py` for isolated logic and `tests/integration/test_*.py` for Neo4j-dependent flows. Mark integration tests with `@pytest.mark.integration`.

### Documentation Discipline

Documentation must be updated in the same change whenever the code changes the developer workflow or runtime expectations.

Update documentation when the change affects:

- project startup or local run flow
- Docker Compose usage, build contexts, or service dependencies
- ports, env vars, credentials, or required local tools
- `Makefile` commands, test commands, or generation commands
- repository structure that is referenced from onboarding or README
- first-run experience for a new contributor

At minimum, check whether the change also requires updates to:

- `README.md`
- `GETTING_STARTED.md`
- `AGENTS.md`
- service-level README files under `services/*/README.md`

If a document is now partially outdated but a full rewrite is unnecessary, prefer a small accurate update over leaving stale instructions in place.

### Go Test Style

The rules below apply specifically to Go unit tests:

- prefer separate test files per service method or handler method, for example `get_test.go`, `delete_test.go`, `update_password_test.go`
- prefer table-driven tests with explicit `args`, expected result, expected error, and mock builder fields
- create a fresh `minimock.Controller` inside each `t.Run`; do not share one controller across table cases
- use repository and infrastructure mocks from `services/*/pkg/mocks`; do not replace them with ad hoc fakes unless there is a clear reason
- do not branch test setup by `tt.name`; each case should fully describe its own mock expectations
- assert behaviour, arguments, and error propagation; do not test internals of `bcrypt`, logging, tracing, or generated code
- when validation fails, explicitly verify that repository methods are not called
- keep happy-path and error-path cases separate and readable rather than collapsing everything into one oversized test
- for service-layer unit tests, avoid `t.Parallel()` by default unless mocks and shared state are clearly isolated and the test remains deterministic
- when a constructor requires unrelated dependencies, pass mocks/stubs but keep assertions focused only on the method under test

## Commit & Pull Request Guidelines

Recent history uses short conventional-style messages such as `feat: ...` and `fix(proto): ...`. Follow `type(scope): summary` where possible. Keep PRs focused, describe service-level impact, list any new env vars or Docker requirements, and link the issue or task. For API changes, mention regenerated protobuf outputs and affected services explicitly.

### Service README Structure and Preservation

Every implemented service must have `services/<name>/README.md`. Use the same
level-two sections, in this order: purpose and capabilities, architecture and
dependencies, project structure, API, data and main flows, configuration, startup,
usage examples, observability and troubleshooting, development and tests,
limitations and future work, related documents. The current service READMEs use
Russian headings; preserve those headings for consistency. Root, documentation
index, and test-kit READMEs serve different purposes and need not mimic a service.

When correcting facts, preserve useful explanations, runnable examples, design
alternatives, and test guidance. Fix the inaccurate passage in place; do not
replace a detailed document with only a list of limitations. Mark proposed flows
and historical snapshots visibly. A general warning at the top does not fix an
incorrect API example or a contradictory diagram inside the document.

Keep shared development rules in this file. CLAUDE.md files add architecture,
service-specific navigation, and testing context; avoid maintaining conflicting
copies of commands/configuration. Link to service READMEs for full runbooks.
