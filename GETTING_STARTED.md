# Getting Started

Эта инструкция поднимает текущие сервисы проекта, включая минимальный HTTP
Gateway для PUT/GET/HEAD/DELETE. Полный S3 HTTP API и SigV4 ещё не реализованы.
Gateway опубликован только на `127.0.0.1` и не должен открываться наружу до
добавления аутентификации и авторизации.

## Требования

- Docker с Compose plugin;
- запуск команд из корня репозитория, где находится `docker-compose.yml`.

Проверка:

```bash
docker --version
docker compose version
```

## Запуск

```bash
docker compose --profile services up --build -d
docker compose ps
```

Профиль `services` запускает:

- одноразовые `migrator-users` и `migrator-metadata`;
- `users`, `auth`, `authz`, `metadata`, `storage-hot`, `storage-cold`, `quota`, `gateway`;
- PostgreSQL для Users и Metadata, Redis, Neo4j, ZooKeeper и Kafka.

Для минимального стенда с Gateway и двумя Storage (HOT и COLD на разных томах)
используйте `make up-gateway`. Команда также поднимет зависимости Metadata.
Kafka ожидает успешной проверки готовности ZooKeeper перед запуском.
Автоматическая проверка PUT/GET/HEAD, классов HOT/COLD, Range и DELETE: `make smoke-gateway` (создаёт свой тестовый bucket). Перед ручным PUT
создайте тестовый bucket по [инструкции Gateway](services/gateway/README.md#примеры-использования).
Объекты доступны по `/objects/{bucket}/{key}`; Swagger расположен отдельно под `/swagger/`.

Миграторы в норме завершаются с кодом 0. Долгоживущие контейнеры должны быть в
состоянии Up/healthy согласно их Compose healthcheck.

Основа СППР запускается отдельно: `make up-sppr`. Команда поднимает SPPR, его
PostgreSQL и мигратор, Kafka и ZooKeeper без остальных прикладных сервисов.
Проверка: `curl -f http://localhost:8086/readyz`. Детали — в
[README SPPR](services/sppr/README.md). Порт БД — 5436; профиль `services`
не включает SPPR.

Для знакомства с данными СППР без Docker запустите из корня `make demo-sppr-data`,
затем `make dataset-sppr`. Вторая команда создаёт `build/sppr-data` с каталогом,
обращениями, признаками, часовыми метками, прогнозами EWMA и manifest.
Существующий каталог не перезаписывается; для повтора задайте
`SPPR_DATA_OUT=build/sppr-data-repeat`. Профили и границы применимости описаны в
[разборе шага 6](docs/sppr-step6.md); там же вход внешнего каталога/событий,
CSV.gz и прореживание train. Перед работой с реальными данными прочтите
[аудит трасс](docs/traces.md).

Gateway producer из отдельного [PR #1](https://github.com/WithSoull/s3-sppr/pull/1) уже есть в checkout.
Для автономной проверки Ingestor используйте integration tests из README; порядок чтения
кода — в [онбординге SPPR](services/sppr/ONBOARDING.md).

Доступен `make smoke-sppr-local`: дополнительная проверка
HTTP → Kafka → PostgreSQL. Она не нужна для автономных тестов Ingestor.

`make up-sppr-migration` поднимает SPPR с включённым исполнителем переносов
(`MIGRATION_ENABLED=true`) вместе с Gateway, Metadata и обеими Storage. API
переносов — `http://localhost:8086/api/migration`. Очистка источников, отменённых
целей и blobs удалённых объектов работает независимо от паузы переносов. Проверка
полного цикла — `make smoke-migration SMOKE_MIGRATION_ARGS="-timeout 12m"`;
быстрый отдельный Compose-стенд — `make smoke-migration-local` (сокращает лимит
чтения и обе задержки очистки согласованно). Примеры curl и условия stand-тестов — в
[README SPPR](services/sppr/README.md#примеры-использования).

## Панель размещения Planner

Planner использует EWMA по умолчанию. Go-предиктор LightGBM выбирается через
`PLANNER_PREDICTOR=lgbm` и `PLANNER_MODEL_DIR`; каталог, read-only mount и
fallback описаны в [README SPPR](services/sppr/README.md#выбор-предиктора-и-каталог-модели).

```bash
make up-sppr-planner
make seed-planner-admin
```

Первый target запускает также Users, Auth, AuthZ и их зависимости. После их
готовности seed создаёт пользователя и выдаёт ADMIN на `system:placement`.
Войдите на `http://localhost:8086/admin` с напечатанными email/password (по
умолчанию `planner-admin@example.test` / `PlannerAdmin123!`, только для тестов).
Можно экспортировать `PLANNER_ADMIN_EMAIL` и `PLANNER_ADMIN_PASSWORD`.
Токен хранится в HttpOnly SameSite=Strict cookie; по истечении нужен новый вход,
выход — кнопка панели. Health открыт, API без токена отвечает 401, без прав 403,
при сбое Auth/AuthZ 503. При включённом Planner доступ включён по умолчанию;
`ADMIN_AUTH_ENABLED=false` допустим только в локальных тестах и даёт warning.

Для проверки всего цикла нужен отдельный стенд с пустым HOT и без активных
миграций: `make smoke-planner`, либо `make smoke-planner-local` для запуска с
короткими задержками очистки. Настройки восстанавливаются, тестовые объекты и
журналы остаются. Детали, адреса/таймауты Auth и известный дефект replay команды
recompute — в [README SPPR](services/sppr/README.md#вход-и-smoke-planner).

## Порты на хосте

Gateway в Compose публикует события обращений в Kafka (`object-read`,
`object-written`); standalone запуск выключает producer по умолчанию. Настройка
`ACCESS_EVENTS_ENABLED=false` отключает его и в Compose. При включённых событиях
Kafka должна быть доступна на старте Gateway. Consumer событий поставляется
отдельно и для запуска Gateway не нужен. См. [Gateway README](services/gateway/README.md#конфигурация).

| Компонент | Адрес |
|---|---|
| Auth | `localhost:50050` |
| AuthZ | `localhost:50051` |
| Metadata | `localhost:50052` |
| Storage | `localhost:50053` |
| Users | `localhost:50054` |
| Quota | `localhost:50055` |
| Gateway HTTP | `127.0.0.1:8080` |
| Gateway Swagger UI | `http://127.0.0.1:8080/swagger/index.html` |
| SPPR health и API переносов (профиль `sppr`) | `127.0.0.1:8086` |
| PostgreSQL SPPR (профиль `sppr`) | `localhost:5436` |
| PostgreSQL Users | `localhost:5432` |
| PostgreSQL Metadata | `localhost:5433` |
| Redis | `localhost:6379` |
| Kafka external listener | `localhost:9092` |
| Neo4j HTTP | `localhost:7474` |
| Neo4j Bolt | `localhost:7687` |

Контейнеры обращаются к Kafka через `kafka:29092`, а не через внешний listener
9092.

## Проверка и диагностика

```bash
docker compose ps
docker compose logs -f metadata
docker compose logs -f quota
docker compose logs -f storage-hot storage-cold
docker compose logs -f gateway
```

Для другого сервиса замените имя в последней команде. Типичные причины сбоя:
занятый порт, не запущенный Docker daemon, недостаток ресурсов или неготовая
зависимость.

Стандартный health endpoint проверяет заявленный приложением статус процесса и не
обязательно выполняет запрос к базе/диску. Для Storage filesystem проверяет
отдельный RPC `DataStorageService.HealthCheck`.

## Остановка

```bash
docker compose --profile services down
```

Удаление volumes также удаляет локальные данные:

```bash
docker compose --profile services down -v
```

## Makefile

Если установлен `make`:

```bash
make up-services
make up-gateway
make down
make down-volumes
make rebuild
```

Для отдельной PostgreSQL интеграционных тестов Metadata:

```bash
make up-e2e
make test-metadata-integration
make down-e2e
```

Или одним запуском:

```bash
make test-metadata-integration-local
```

Observability поднимается отдельным профилем:

```bash
make up-observability
```

## Разработка вне контейнера

| Компонент | Нужные инструменты | Инструкция |
|---|---|---|
| Go services | Go 1.24.1; зависимости в Docker | [Auth](services/auth/README.md#запуск), [Users](services/users/README.md#запуск), [Metadata](services/metadata/README.md#запуск), [Storage](services/storage/README.md#запуск), [Gateway](services/gateway/README.md#запуск) |
| AuthZ | Python 3.12, venv, pip | [Setup и отдельный invalidator](services/authz/README.md#запуск) |
| Quota | Rust (Docker build использует 1.95), protoc, Redis | [Запуск и tests](services/quota/README.md#запуск) |
| Ручные RPC | grpcurl | раздел «Примеры использования» каждого сервиса |
| Генерация proto | protoc, Go, Python/pip, make | команды ниже |

Go `.env` читается относительно папки запуска, Python Config — только из окружения,
Rust использует dotenvy от текущей папки. Для local process адреса Docker DNS
заменяются на localhost и опубликованные порты. Если контейнер сервиса уже запущен,
остановите именно его, чтобы освободить порт; зависимости оставьте работающими.

## Изменение protobuf

Из корня:

```bash
make install-deps
make generate
```

`install-deps` кладёт Go plugins и Python grpcio-tools в `bin/`; сам `protoc`
нужно установить отдельно. Go/Python clients генерируются в `shared/pkg/go` и
`shared/pkg/py`. Для отдельного сервиса есть targets `generate-<service>-go/py`;
у Users имя target — `generate-user-go/py`. Rust Quota генерирует bindings через
`build.rs` при сборке и также требует `protoc`.

## Что проверять при первом запуске

1. `docker compose ps -a`: миграторы должны завершиться с кодом 0; healthcheck
   есть не у каждого приложения, отсутствие метки healthy не равно ошибке.
2. Standard gRPC Health показывает статус процесса. Custom HealthCheck у Metadata
   проверяет PostgreSQL/Kafka, AuthZ — Neo4j/Redis, Storage — filesystem, Quota — Redis.
3. `make up-observability` поднимает Collector и UI. Ошибка экспорта telemetry
   при отсутствии Collector не является доказательством ошибки бизнес-RPC.
4. При ошибке credentials БД проверьте соответствие `.env` и существующего volume:
   новые значения env не переинициализируют уже созданную БД. `down -v` удаляет данные.
5. Проверьте реальный запрос из README сервиса. Успех health сам по себе не является
   end-to-end проверкой записи объекта.

[Карта документации](docs/README.md) связывает сервисы, архитектурные планы и аудиты.
