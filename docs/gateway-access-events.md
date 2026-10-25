# Gateway → Kafka → SPPR: требования и приёмка

## Цель и границы

После HTTP PUT/GET Gateway публикует событие обращения в `opens3.access.v1`.
SPPR сохраняет историю, определяет пропуски и считает спрос по успешным
клиентским чтениям. Producer дополняет существующий data path Metadata/Storage.

Основание: текущий контракт Ingestor и требование шага 2.5 проекта — ограниченная
асинхронная очередь, отсутствие ожидания Kafka в HTTP и обнаружение потерь по
номерам. Здесь фиксируются условия реализации и проверки этого изменения.

В scope входят общий JSON-контракт, producer, HTTP outcome, DI/config/lifecycle,
метрики, документация и проверка Gateway → Kafka. Auth/AuthZ/Quota,
Range, выбор HOT/COLD, durable outbox, heartbeat, общий watermark, прогнозы и
переносы не входят. SPPR Ingestor поставляется отдельным независимым PR в main.
Оба PR включают один и тот же небольшой коммит JSON-контракта; каждый можно
собирать и мержить первым. Проверка HTTP → Kafka → PostgreSQL требует обоих PR.

## Контракт события

| Поле | Требование |
|---|---|
| `schema_version` | 1, общий Go JSON type и validator для Gateway и SPPR |
| `gateway_id` | стабильная конфигурация экземпляра; `[A-Za-z0-9_.-]{1,128}` |
| `stream_id` | новый случайный ID при запуске producer; тот же формат |
| `sequence` | int64 от 1; назначается до попытки enqueue, включая drop |
| `occurred_at` | время завершения операции; UTC, микросекунды, не убывает по sequence |
| `bucket`, `object_key` | идентичность из HTTP route, UTF-8 без NUL, до 255/1024 байт |
| `version_id` | известная версия Metadata, обязательна при success, до 256 байт |
| `operation` | `read` для GET, `write` для PUT |
| `origin` | текущий HTTP всегда `client`; `migration` зарезервирован для внутренних операций |
| `result` | `success` либо `error`, независимо от уже отправленного HTTP200 |
| `bytes` | неотрицательное фактически наблюдаемое количество переданных байтов |

Kafka key равен `gateway_id/stream_id`. Один поток закреплён за одной partition
на всю жизнь producer, включая расширение topic. Kafka offset и sequence
производителя — разные идентификаторы. Повторная отправка сохраняет те же поля
и trace context. W3C `traceparent` передаётся в Kafka headers.

## Когда формируется outcome

- GET success: Metadata разрешила версию, Storage подтвердил полный EOF, Gateway
  успешно записал клиентскому HTTP writer ровно ожидаемый объём. Reader или
  отправленный HTTP200 сами по себе success не означают.
- Пустой GET success: подтверждён EOF. Это чтение с нулём байтов.
- Обрыв GET, ошибка writer/Storage и отмена запроса: error и число успешно
  записанных HTTP writer байтов. Существующий abort через `http.ErrAbortHandler`
  сохраняется. Gateway не способен доказать получение байтов клиентским
  приложением после успешного Write: это граница наблюдаемости.
- PUT success: Storage завершил запись и Metadata вернула зарегистрированную
  версию. Отмена клиента после Storage commit не отменяет регистрацию и outcome.
- PUT error: исходная ошибка сохраняет прежний HTTP mapping. `bytes` отражает
  байты, прочитанные обработчиком из HTTP body; это не обещание durable commit
  на Storage. Неизвестная версия остаётся пустой.
- Нет событий для health, Swagger, неподдерживаемых методов и некорректных путей.
  Корректный object route с отсутствующим объектом/bucket даёт error event.
- Ровно одно событие на завершившуюся обрабатываемую операцию. Ошибка analytics
  не меняет уже выполненную объектную операцию и не заменяет её ответ.

## Очередь и доставка

Назначение номера, нормализация времени и enqueue выполняются в одном порядке
под синхронизацией. Если часы отступили назад, время ограничивается предыдущим
значением. Конкурентные запросы не могут поменять местами sequence при enqueue.

Очередь имеет конечную ёмкость. При заполнении новое событие отбрасывается,
его sequence уже израсходован. HTTP не ждёт освобождения места или Kafka.
Следующее доставленное событие позволяет Ingestor обнаружить gap.

Один отправитель повторяет head до подтверждения либо остановки. Следующие
события не обгоняют ошибку. Используются shared go-kit producer и Sarama с
idempotence, `acks=all`, одним in-flight request и ограниченными сетевыми
таймаутами. Ошибка публикации не останавливает HTTP сервер.

Это очередь в памяти: нет гарантии доставки при аварии процесса. Потерянный
хвост без следующего события не обнаруживается по sequence. Новый stream_id
не восстанавливает хвост старого потока. Переполнение и незавершённый shutdown
видны по логам/метрикам, но не заменяют механизм определения полноты истории.

## Конфигурация и lifecycle

Producer опционален и выключен по умолчанию при отдельном запуске Gateway.
Выключенное состояние не создаёт Kafka client и worker. Включённое требует
Gateway ID, broker/topic и положительных параметров очереди, retry и timeout.

Provider собирает зависимости в принятой структуре сервиса. Startup с enabled
producer ограничен сетевыми таймаутами; недоступность Kafka при создании клиента
может завершать startup с явной ошибкой. Runtime outage обрабатывается очередью.

Shutdown сначала прекращает HTTP admission и завершает активные handlers,
затем прекращает enqueue, пытается доставить pending до deadline и закрывает
Kafka resources. Отправитель завершается; потерянные pending учитываются.
Transport cleanup получает отдельный бюджет `ACCESS_EVENTS_SHUTDOWN_TIMEOUT`,
чтобы истёкший deadline очереди не отменил ожидание completion-router.
Producer не закрывается конкурентно с ещё работающими handlers или telemetry.
Частичная инициализация также освобождает созданные зависимости.

Метрики используют конечные категории enqueue/send/retry/drop и причину drop.
Object key, Gateway ID и stream_id не используются как labels. Сообщение retry
не должно заново сериализовываться с другим номером или временем.

## Приёмка

Producer проверяется без Ingestor: unit/component tests и собственный Kafka
transport не требуют PostgreSQL SPPR. В строках про спрос, gaps и Ingestor ниже
описана дополнительная сквозная приёмка после объединения обоих PR.

| Сценарий | Ожидаемый результат | Уровень проверки |
|---|---|---|
| Обычный, пустой и chunked PUT/GET | корректные version/result/bytes; только GET повышает спрос | handler tests + реальный smoke |
| Ошибка Metadata/Storage, partial GET, writer error | error event; прежний HTTP response/abort | unit/component |
| Конкурентное завершение запросов | queue и Kafka сохраняют последовательность и время | producer race tests |
| Ошибка Kafka и повтор | HTTP не ожидает Kafka; отправляется тот же head | controlled producer tests |
| Переполнение | drop; номер израсходован; последующее событие показывает gap | producer tests + Ingestor integration |
| Повтор доставленного события | спрос не увеличивается | PostgreSQL integration + smoke replay |
| Рестарт | новый stream_id, sequence снова 1 | producer tests |
| Shutdown с pending и заблокированным broker | ограниченное ожидание, отсутствие гонок и утечек worker | lifecycle/race tests |
| Trace context | HTTP parent проходит через Kafka header в Ingestor | producer/handler checks |

В итоговом PR перечисляются только фактически выполненные проверки. Результат
smoke с настоящими сервисами отделяется от unit tests с mocks и проверки CI.

Исправления финального ревью и результаты запусков:
[отчёт от 25.10.2026](gateway-access-events-review.md).
