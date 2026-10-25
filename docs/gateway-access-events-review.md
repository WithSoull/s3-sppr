# Gateway access events: ревью и проверки 25.10.2026

Требования и критерии приёмки — в [gateway-access-events.md](gateway-access-events.md).
Этот файл фиксирует выполненную проверку producer и его интеграции с Ingestor.

## Исправлено при ревью

| Проблема | Исправление |
|---|---|
| Проверка `closed` отдельно от отправки не защищала adapter от concurrent Close и записи в закрытый Input. | Admission защищена RWMutex и closing channel; Close разблокирует ожидающих отправителей до AsyncClose. Тест останавливает transport, отменяет первое ожидание, блокирует вторую отправку и закрывает adapter. |
| Drain мог израсходовать context до закрытия metrics/traces/logger. | Context остальной cleanup создаётся после producer drain. Transport получает отдельный бюджет на завершение in-flight и completion-router. |
| Отмена Send возвращает управление до окончательной доставки. Буферы вызывающего могут быть переиспользованы; поздний ack может относиться к другой попытке. | Adapter копирует key/value и коррелирует completion через Metadata конкретного сообщения. Тест меняет исходные буферы после отмены и проверяет изоляцию позднего ack. |
| После ошибки Storage Retrieve терялась уже полученная версия Metadata. | Service возвращает известную metadata вместе с ошибкой; GET error event сохраняет version_id. |
| Topic validation пропускала недопустимые символы, а bootstrap — нечисловой port. | Проверяются допустимый алфавит topic, исключения `.`/`..`, host и port 1–65535. |
| Sampling drop logs не давал точного размера недоставленного хвоста при shutdown. | Перед отменой sender выводится `pending_events`; каждый drop отражается в counter. |
| Сообщение об успешной остановке всего Gateway появлялось до попытки выгрузить очередь. | Лог сообщает о завершении HTTP drain; общий результат включает ошибки producer shutdown. |

Сохранены существующий HTTP error mapping, EOF/final-byte verification и
регистрация Metadata после Storage commit при отмене клиента. JSON-контракт
вынесен в shared без изменения полей v1. Протокол protobuf не менялся.

## Автоматические проверки

Для независимого producer PR выполняются:

```bash
go test -race ./shared/pkg/go-kit/kafka/producer ./shared/pkg/go-kit/events/access \
  ./services/gateway/... -count=1
go vet ./services/gateway/... \
  ./shared/pkg/go-kit/kafka/producer ./shared/pkg/go-kit/events/access
docker compose --profile services config --quiet
docker build -f services/gateway/Dockerfile -t codex-gateway-events-review:20261025 .
git diff --check
```

Тесты проверяют outcome/bytes для обычных, пустых, chunked и неуспешных операций,
обрыв GET и ошибку writer, порядок конкурентных Record, одинаковый payload при
retry, переполнение очереди, cancellation и shutdown. Integration suite проверяет
SQL rollback, deduplication, конкуренцию, gaps и Kafka offsets с `-race`.

## Историческая сквозная проверка до разделения PR

Ниже сохранены результаты ранее проверенного объединённого среза. Ingestor,
его PostgreSQL, integration tests и сквозной smoke поставляются отдельным PR;
это не зависимости сборки или запуска producer.

Gateway и SPPR запускались как отдельные Go-процессы на свободных портах;
Metadata/Storage — работающие локальные контейнеры. SPPR использовал отдельную
PostgreSQL. Для outage поднят отдельный Kafka KRaft, поэтому общий broker и
работающий Gateway проекта не останавливались.

- Smoke выполнил 3 PUT и 3 GET: пустой объект, binary и chunked upload; отдельно
  выполнен GET отсутствующего объекта. События и версии совпали с HTTP outcome.
  Спрос составил 3 чтения / 8518 байт; writes и error event его не увеличили.
- Точное переиздание Gateway event не увеличило спрос. В отдельном synthetic
  потоке `origin=migration` события 1 и 3 создали gap `2..2` без роста спроса.
- При расширении topic с 3 до 5 partitions действующий Gateway stream сохранил
  прежнюю partition; следующий sequence обработался без ошибки перемещения.
- В запрос передан известный `traceparent`. OTLP receiver получил span
  `access.ingest` с тем же trace ID; parent — HTTP span Gateway.
- При остановленном тестовом Kafka и очереди ёмкостью 2 все 20 GET вернули 200
  и ожидаемое тело. Измеренные времена этой серии — 0.94–18.54 ms. Это проверка
  отсутствия ожидания broker, а не оценка p95 или производительности.
- После восстановления Kafka сохранённые события доставлены; следующее реальное
  обращение привело к gap `11..27`, `missing_events=17`.
- При недоступном Kafka и одном pending event SIGTERM завершил Gateway за 2.01 s
  при drain timeout 2 s. Выведены `pending_events=1` и ошибка deadline; процесс
  завершился с ненулевым кодом, чтобы не скрыть недоставленный хвост.
- После рестарта тот же gateway_id получил новый stream_id и начал с sequence 1.

Smoke проверен на работающих зависимостях с явными адресами. Полный новый
Compose-стенд на стандартных портах локально не пересоздавался, чтобы не
затронуть существующие контейнеры. Объединённый CI также прошёл сквозной smoke
до разделения. Producer CI после разделения проверяет только Gateway/shared
race tests и собственный real-service PUT/GET smoke; Ingestor проверяется отдельно.

## Границы результата

Нет durable outbox, heartbeat, общего watermark и восстановления потерянного
хвоста. Очередь в памяти сознательно допускает потери, а номер и drop counter
позволяют наблюдать часть этих потерь. Полнота истории и ресурсы под нагрузкой
этим изменением не доказаны.
