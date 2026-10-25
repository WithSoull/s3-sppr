# Ревью SPPR от 25 октября 2026 года

Исторический снимок до подключения producer Gateway. Последующее изменение
реализует его в отдельном [PR #1](https://github.com/WithSoull/s3-sppr/pull/1); текущий
runbook и состояние сервиса описаны в [README](README.md). Результаты проверок
ниже относятся к первоначальному ревью Ingestor.

## Объём и результат

База репозитория обновлена fast-forward с `6c1cbb6` до `e56dbb8` (`origin/main`).
Локальные незакоммиченные изменения SPPR сохранены и восстановлены поверх неё.
Конфликт README разрешён с сохранением описаний Gateway и SPPR. Резервная копия
исходного рабочего дерева оставлена в stash с сообщением
`codex-before-sppr-review-20261025`.

Прочитаны весь runtime-код `services/sppr`, unit/integration tests, SQL,
Dockerfile, workflow, примеры и конфигурация. Дополнительно проверены используемые
PG/transaction/Kafka/logger компоненты go-kit и текущий data path Gateway.
Generated mocks проверены как зависимости тестов; их генератор не переписан.
Полный независимый аудит остальных сервисов в этот scope не входил.

SPPR реализует Ingestor, сохраняющий историю и минутный спрос. Основной алгоритм
сохранения, дедупликации и блокировки при ошибке подтверждён тестами с реальными
PostgreSQL и Kafka. Дальнейшие блоки СППР ещё не реализованы. Gateway существует,
но producer `AccessEvent` в нём пока отсутствует.

## Исправленные проблемы

| Приоритет | Проблема до исправления | Изменение и проверка |
|---|---|---|
| P2 | `App.Close` вызывал только `Server.Close`, если HTTP server уже создан. До первого `Serve` server ещё не владел listener, и порт оставался занят. | Listener закрывается явно; повторное закрытие уже закрытого listener не считается ошибкой. `close_test.go` проверяет повторное занятие того же порта после Close без Run. |
| P2 | Завершение вызывало только `logger.Sync`; optional OTLP provider не закрывался. Последние логи могли потеряться, в том числе при неуспешном startup. | App учитывает инициализацию logger и вызывает `logger.Shutdown` последним. Integration smoke принимает OTLP-логи и проверяет их выгрузку после Close. |
| P2 | `KAFKA_PROCESS_TIMEOUT` одновременно задавал deadline обработки и сетевые таймауты Sarama. При уменьшении времени обработки можно было обрывать штатные Fetch/JoinGroup ожидания брокера. | Сетевые overrides удалены; используются defaults Sarama. Smoke запускается с обработкой по 100ms, независимо от broker long polling и присоединения к группе. |
| P2 | Fallback DSN собирался через `fmt.Sprintf` без escaping. Пробел, кавычка или обратный слеш в пароле меняли разбор строки подключения. | Подключение собирается через `url.URL`, `UserPassword` и `JoinHostPort`. Тест разбирает результат настоящим pgx parser и сверяет исходные значения, включая IPv6. |
| P2 | Integration tests дописывали `search_path` к URL. Если параметр уже был задан, pgx выбирал первое значение и тесты могли использовать чужую схему. Изменение `RuntimeParams` перед `ConnString()` также не меняло исходную DSN. | URL query обновляется через `Set`; неработавшая правка `RuntimeParams` удалена. Полный integration suite прошёл с исходным `search_path=public`; тестовые таблицы создаются в собственных схемах. |

Приоритеты обозначают влияние найденной ошибки до исправления. Все перечисленные
исправления находятся в рабочем дереве.

## Чистка кода и тестов

- Удалена недостижимая проверка `sarama.ErrClosedConsumerGroup` в Worker:
  shared consumer уже преобразует её в `nil`.
- Удалён отдельный service unit case `duplicate`: он повторял проверку проброса
  результата. Дедупликация по-прежнему проверяется на реальной БД, через Kafka и
  в handler для результата telemetry.
- Удалён assert `elapsed >= 0`, который не проверял полезное поведение.
- В существующий успешный service case добавлена проверка нормализации времени.
- Удалены комментарии, пересказывающие соседнюю строку кода, и лишний Sync в main.
- Dockerfile использует cache mounts и скачивает зависимости до копирования
  исходников, как новые Dockerfile соседних Go-сервисов.
- CI ограничен `contents: read` и также запускается при изменениях generated Go
  contracts, от которых могут зависеть используемые компоненты shared.

Сохранены service provider, интерфейсы и generated minimock: это принятый в
репозитории способ сборки и тестирования зависимостей. Сохранены тесты rollback,
конкурентной доставки, gaps и Kafka offsets: они проверяют разные гарантии.
Число строк само по себе не использовалось как критерий удаления.

## Выполненные проверки

Все команды запускались из корня репозитория. Поднят отдельный PostgreSQL SPPR
на 5436; Kafka использовалась по localhost:9092. Integration tests создавали
собственные schemas и уникальный topic. Прикладные данные других сервисов не
изменялись. После проверки оставшихся тестовых schemas и таблиц в public — ноль;
поднятая для ревью PostgreSQL остановлена.

```bash
make test-sppr
go vet ./services/sppr/...
go test ./services/gateway/... -count=1

SPPR_TEST_PG_DSN='postgres://sppr_user:sppr_password@localhost:5436/sppr_db?sslmode=disable&search_path=public' \
SPPR_TEST_KAFKA_BOOTSTRAP=localhost:9092 \
make test-sppr-integration

docker compose --profile sppr config --quiet
docker build -f services/sppr/Dockerfile -t codex-sppr-review:20261025 .
docker run --rm --entrypoint id codex-sppr-review:20261025
git diff --check
gofmt -l services/sppr
```

Подтверждены unit/integration tests SPPR с `-race`, unit tests Gateway, vet, разбор Compose и сборка
Dockerfile. Runtime user образа — uid/gid 10001. Kafka smoke проверил повтор,
gap, сбой записи и восстановление, 503 readiness при живом healthz, остановку на
poison message, offset перед ним, экспорт traces/metrics/logs и shutdown.

Сам Ingestor запускался внутри integration test. Проверка образа состояла из
сборки и проверки пользователя; полный `make up-sppr` с отдельным broker заново
не запускался, поскольку 9092 уже занимал работающий стенд. CI workflow локально
не исполнялся в GitHub Actions. Сквозной HTTP Gateway → Kafka → SPPR не проверен:
его producer ещё не реализован.

## Оставшиеся ограничения

Это известные границы реализации, а не утверждение о завершённости всей СППР:

- Нет producer Gateway, поэтому реальные PUT/GET не формируют этот поток событий.
- Нет общего watermark, heartbeat и способа обнаружить потерянный хвост потока.
- Поздний event из gap и конфликт payload блокируют partition. Восстановление
  gaps, DLQ и административный пропуск пока отсутствуют.
- Readiness не гарантирует полноту истории; она сообщает о работе ingestion.
- Startup проверяет наличие таблиц, но не полный состав схемы и версии миграций.
- Нет retention/partitioning истории и замеров производительности или лимита
  ресурсов. Kafka SASL/TLS в конфигурации сервиса не предусмотрены.
- Feature Builder, Predictor, Model Manager, Planner и перемещения ещё отсутствуют.

Порядок чтения и воспроизведение поведения собраны в [ONBOARDING.md](ONBOARDING.md).
