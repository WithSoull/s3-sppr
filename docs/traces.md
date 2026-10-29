# Трассы: аудит доступа и преобразование

Обновлено 28.10.2026. Исходный аудит Meta выполнен через S3 ListObjectsV2,
HEAD и Range. Локальные IBM COS 000/001/002 предоставлены автором, принявшим
SNIA Trace Data Files Download License. Конвертер ничего не скачивает и
не принимает условий; исходники читаются как данные, без извлечения в репозиторий.

| Источник | Доступ и размер | Формат и поля | Лицензия/условия | Статус |
|---|---|---|---|---|
| Meta CDN, март 2023 | Публичный [S3 CacheMon](https://cache-datasets.s3.amazonaws.com/?list-type=2&prefix=cache_dataset_txt/2023_metaCDN/), без регистрации; три файла ниже | zstd CSV с заголовком: timestamp (Unix ms), cacheKey, OpType, objectSize, responseSize, responseHeaderSize, rangeStart/End, TTL, SamplingRate, cache_hit, item_value, RequestHandler, cdn_content_type_id, vip_type | [Оригинал CacheLib](https://cachelib.org/docs/Cache_Library_User_Guides/Cachebench_FB_HW_eval/) разрешает свободное скачивание под лицензией CacheLib ([Apache-2.0](https://github.com/facebook/CacheLib/blob/main/LICENSE)); [коллекция CacheMon](https://github.com/cacheMon/cache_dataset#license) объявляет CC BY 4.0, требует attribution, ссылку и описание изменений | Доступна; конвертер ниже. HTTP status отсутствует — успех GET не доказуем напрямую |
| IBM Cloud Object Storage, HotStorage 2020 | [SNIA IOTTA 36305](https://iotta.snia.org/traces/key-value/36305); локальные 000/001/002 | Подтверждено из файлов: несмотря на `.tgz`, Unix compress → tar → CRLF text. `relative_ms REST.OP.OBJECT object_id size [start end]`; DELETE может не иметь size | [SNIA Download License](https://iotta.snia.org/repository/download_license), принята поставщиком файлов | Потоковый `-source ibm-cos`; полный 001/002, 000 с key-share 0.1 проверены; статус успеха отсутствует |
| IBM Docker Registry, FAST 2018 | [DRTP](https://dssl.cs.vt.edu/drtp/) → [архив](https://drive.google.com/file/d/1FjgyCJ8YayOKeMlxtbznhsiXGPYC9yP8/view), 1 662 385 619 байт | Подтверждён tar.gz: 7 zone directories, 23 075 133 724 байта JSON; pretty-printed arrays, flat dotted field names, UTC timestamps; имена в syd01 имеют prefix `prod-syd01-logstash-...` | `not stated; used with the authors' permission`; лицензия replayer не лицензирует данные | Потоковый `-source docker -zone syd01`, полный syd01 проверен; HTTP status присутствует |

CacheMon README заявляет IBM Docker, но ведёт на общий prefix. В проверенных
листингах `cache_dataset_txt/`, `cache_dataset_oracleGeneral/` (включая `other/`)
IBM Docker и IBM COS не обнаружены. Это не подтверждённые работающие зеркала.
`2017_systor` содержит LUN/block traces, это другой источник.

Meta: все имена относительно
`https://cache-datasets.s3.amazonaws.com/cache_dataset_txt/2023_metaCDN/`:

| Файл | Сжатый размер, байт | Доля из имени |
|---|---:|---:|
| `reag0c01_20230315_20230322_0.2000.csv.zst` | 632 922 712 | 0,2 |
| `rnha0c01_20230315_20230322_0.8000.csv.zst` | 1 656 286 630 | 0,8 |
| `rprn0c01_20230315_20230322_0.2000.csv.zst` | 1 442 532 807 | 0,2 |

Рядом лежат три небольших `cdn_trace_<region>_20230315_20230322.json` с
конфигурацией CacheBench. Доля из имени не равна traffic factor кластера или
построчному SamplingRate; эти величины нельзя перемножать без модели сбора.
Семантика `OpType=1` подтверждена [Request.h CacheBench](https://github.com/facebook/CacheLib/blob/main/cachelib/cachebench/util/Request.h): `kSet=0`, затем `kGet`.

## IBM COS: преобразование и проверка

`-source` по умолчанию `meta`: прежние Meta flags и поведение сохранены.
COS требует `-origin` RFC3339 и `-allow-inferred-success`. Для измерений выбран
**2020-01-01T00:00:00Z** как искусственный ноль; это не дата захвата 2019 года.
Час суток / день недели после такого переноса не являются реальным календарём.
Один архив = один текстовый trace = один bucket (`ibm-cos-000`, `001`, `002`).
Помимо gzip tar поддерживается реальный Unix compress tar через `/usr/bin/gzip -cd`
(доверенная системная программа, без shell); необжатый sample читается как текст.
Архивные пути не извлекаются; многогигабайтный member разбирается построчно.
Память хранит каталог выбранных версий и разные ID для COPY-аудита,
а не строки исходника или все запросы. Хеш относится ко всему исходному сжатому файлу, включая tar padding.

Mapping / row policy:

- `relative_ms` → `origin + ms`, `object_id` → общий `mapKey`, выборка через
  общий `SampleKey(original_id, seed, key_share)`. Счётчики не масштабируются.
- GET — один `read/client/success` на запрос, независимо от range. Успех
  предполагается только после opt-in. Bytes = size или длина inclusive range,
  конец ограничивается `size-1`. Реальные хвостовые запросы могут выходить за
  EOF (000: 1 053 строки); пустой объект имеет `size=0, range=0 -1`, Bytes=0.
- Каждый PUT создаёт следующую версию SHA256(id NUL generation), в том числе
  при прежнем размере. Предыдущая версия закрывается в момент PUT.
- DELETE закрывает текущую версию; неизвестный DELETE учитывается отдельно.
  Его size может отсутствовать, поэтому версия с выдуманным нулевым размером
  не создаётся. Начальное наблюдение HEAD/GET без PUT создаёт версию с `CreatedAt=origin`.
  Повторное наблюдение после DELETE без PUT начинает версию в момент наблюдения.
- HEAD исключён из demand, но может установить начальный размер в каталоге.
  HEAD не меняет размер активной версии. GET с конфликтующим размером пропускается
  и учитывается; скрытый overwrite не выдумывается.
- COPY исключён целиком. Во **всём 000 до key sampling**: 10 800 COPY, 10 740
  разных ID; 10 782 COPY (99,83%) имеют ранее наблюдавшийся ID (включая HEAD),
  после 494 COPY (4,57%) позднее есть GET того же ID; таких ID 435 (4,05%).
  Это временная связь с ID, а не доказательство source/destination или успеха.
  Поэтому COPY не является ни чтением, ни созданием версии. Пропущенные эффекты
  COPY остаются ограничением восстановленного каталога.
- Плохие timestamps/key/size/range учитываются и пропускаются; нарушение порядка
  времени, некорректная структура, несколько regular members или повреждение
  сжатия — ошибка всего запуска. Равные timestamps не дедуплицируются.
  PUT/DELETE в ту же миллисекунду, что предыдущее чтение своей версии, — ошибка:
  такой порядок нельзя точно представить полуоткрытым lifetime.

Проверка 28.10.2026 (seed=0, origin как выше; Darwin, wall из `/usr/bin/time -l`):

| Trace | key-share | Строк всего | GET / PUT / HEAD / DELETE / COPY | Events / versions | Wall, s | Peak RSS, MiB |
|---|---:|---:|---|---|---:|---:|
| 000 | 0.1 | 45 822 733 | 962 187 / 66 010 / 44 760 170 / 23 566 / 10 800 | 92 666 / 20 920 | 45.08 | 64.33 |
| 001 | 1 | 1 577 730 | 468 959 / 629 333 / 479 438 / 0 / 0 | 468 959 / 1 085 298 | 4.77 | 727.20 |
| 002 | 1 | 376 495 | 376 250 / 126 / 0 / 119 / 0 | 376 250 / 407 | 0.93 | 12.00 |

В песочнице `time -l` после wall печатает `sysctl kern.clockrate: Operation not
permitted` и сам возвращает 1 даже после успешной публикации результата.
RSS измерен отдельно через Python `resource.getrusage(RUSAGE_CHILDREN).ru_maxrss`
(Darwin: байты, потом /1048576), включая дочерние gzip; это не вывод RSS time.
000 выполнялся одновременно с dataset; wall не является изолированным benchmark.
Результаты и ресурсные логи находятся только в gitignored `build/`.

Из корня репозитория, имена out должны ещё не существовать:

```bash
go build -o build/trace-convert ./services/sppr/cmd/trace-convert
/usr/bin/time -l build/trace-convert -source ibm-cos \
  -input /Users/grishinid/home/98_Downloads/IBMObjectStoreTrace001Part0.tgz \
  -bucket ibm-cos-001 -origin 2020-01-01T00:00:00Z \
  -allow-inferred-success -out build/ibm-cos-001
# Аналогично 002. Для 000 дополнительно -key-share 0.1.
```

Dataset получает существующие `catalog.json` / `events.jsonl` без промежуточного
формата. Границы задаёт вызывающий: min/max в manifest не являются watermark.
Для полного 001 команда проверки (15m grid, чтобы ограничить развёртку миллиона
версий; это отличается от default 5m):

```bash
GOMAXPROCS=2 GOMEMLIMIT=2GiB /usr/bin/time -l go run ./services/sppr/cmd/dataset \
  -catalog build/ibm-cos-001/catalog.json -events build/ibm-cos-001/events.jsonl \
  -start 2020-01-01T00:20:44.855Z -end 2020-01-08T00:43:17.614Z \
  -train-end 2020-01-06T00:20:44.855Z -validation-end 2020-01-07T00:20:44.855Z \
  -format csv.gz -train-every 15m -snapshot-every 15m -out build/ibm-cos-001-dataset
```

Полный dataset 001 завершён после перезапуска: **539 337 365 строк**,
468 959 событий. Wall `/usr/bin/time -l` **6353,55 s**, peak RSS через
getrusage **2725,14 MiB**, exit code самого `go run` — 0. Запуск:
`GOMAXPROCS=2 GOMEMLIMIT=2GiB` (мягкая цель Go GC, не жёсткий предел RSS);
CSV статистика считалась параллельно отдельным процессом. Ресурсные ограничения
`time -l` / sysctl и метод измерения RSS описаны выше.

| Split | Строк | Известных labels | Нулевых labels | Доля нулевых среди известных |
|---|---:|---:|---:|---:|
| train | 359 057 539 | 359 057 539 | 357 203 276 | 99,483575% |
| validation | 80 047 897 | 80 047 897 | 80 047 897 | 100% |
| test | 90 777 663 | 90 777 663 | 90 777 659 | 99,999996% |
| purged | 9 454 266 | 5 128 584 | 5 128 584 | 100% |

Доля нулевых во всех известных train/validation/test labels вместе —
**99,650061%** (528 028 832 / 529 883 099), включая ineligible-строки.
Eligible: train 298 912 549, validation 80 047 897, test 90 777 663.
Неполная история признаков не делает строку пригодной для обучения даже при
известной метке. У 4 325 682 purged-строк label неизвестен, он не считается
нулём. Четыре положительные test-строки сохранены; округление процента почти
до 100% не означает полного отсутствия чтений.

Каждый CSV прочитан потоково с проверкой gzip; counts сверены с manifest.
Независимый расчёт по событиям, lifetime и сетке срезов совпал для каждого split.
Нули означают отсутствие наблюдённых выбранных GET при принятой модели успеха
и покрытия, а не доказанный нулевой production demand. Полный каталог даёт
большую развёртку: 1 085 298 версий, в том числе PUT-only объекты без чтений.

## IBM Docker Registry: преобразование и проверка

Локальный архив tar.gz читается целиком потоково для проверки SHA-256, но JSON
разбирается только в выбранной зоне / датах. Ничего не извлекается: tar paths
не становятся путями файлов. В исходнике 7 зон: `dev-mon01`, `fra02`,
`prestage-mon01`, `stage-dal09`, `lon02`, `dal09`, `syd01`.
Поддерживаются `<zone>-logstash-YYYY.MM.DD-N.json` и реальный
`prod-<zone>-logstash-YYYY.MM.DD-N.json`. `-from YYYY-MM-DD` включительно,
`-to YYYY-MM-DD` исключительно фильтруют **дату имени файла**, не timestamp
записи. В syd01 380 файлов, 607 511 933 байта распакованного JSON;
наблюдаемые timestamps: 2017-07-01T00:00:16.931Z .. 2017-09-03T23:59:54.125Z.
Поля `http.request.*` / `http.response.*` — плоские JSON keys с точками.
Некоторые записи не имеют `http.response.written` (например uploads).

Mapping и решения:

- Объекты — только image layer blobs. URI допускает один или несколько
  компонентов repo между `v2/` и `/blobs/<digest>` (встречаются оба варианта).
  Ключ — **digest**, общий для репозиториев в одной зоне, bucket `docker-<zone>`.
  Применяются общие `mapKey` / `SampleKey(original_digest, seed, key_share)`.
- Из 13 556 digest во всех blob observations у 554 несколько repo. У 546
  полные GET 200 есть в нескольких repo: **0 расхождений максимального размера
  между repo**. Полные GET покрывают 15 182 repo/digest URI, но только 12 587
  разных digest. Это поддерживает дедупликацию по digest в проверенном syd01;
  сохранение равенства при анонимизации остаётся предположением для других зон.
- GET blobs со статусом 200 или 206 — один `read/client/success` с Bytes из
  `http.response.written`. Здесь статус известен, opt-in не требуется.
- Размер неизменяемого blob = **максимум written полного GET 200**, независимо
  от порядка файлов/записей. 206 никогда не задаёт полный размер. В syd01
  16 digest имеют конфликт полных размеров: 19 наблюдений ниже итогового max.
  Все такие чтения сохраняются со своими Bytes; конфликт не создаёт версию.
  Это ретроспективный выбор metadata, а не известный в прошлом размер. Max может
  завышать реальный размер при ошибочных исходных данных.
- Одна версия SHA256(digest NUL 1). CreatedAt = минимальный timestamp успешного
  blob observation в обработанном диапазоне, включая HEAD. Это не upload time:
  upload URI не содержит digest. Нет полной 200-size — нет версии / чтений;
  в syd01 969 digest наблюдались без полного размера, пропущенных 206-only reads 0.
- HEAD, manifests, uploads, pings, другие маршруты/методы, non-2xx исключены.
  Counts содержат route/method/status до sampling; `non_2xx` пересекается с
  route exclusions (23 ответа 301 — ping). Прочие причины пропуска — first-match,
  как описано в manifest. GET другого 2xx не считается чтением.
- Записи не упорядочены: 317 746 регрессий timestamp в порядке обхода архива.
  Конвертер пишет компактный spool и запускает доверенный `/usr/bin/sort`
  (`LC_ALL=C`, `-S 32M`), ключ UTC timestamp + исходный ordinal. Память каталога и аудита зависит от числа
  разных объектов и repo/digest, а не от числа повторных запросов; диск нужен пропорционально выбранным GET. Одновременные
  запросы сохраняются отдельно с последовательным Sequence после сортировки.
- Некорректная JSON-структура, хвост после массива, повреждение tar/gzip,
  ошибка I/O или sort прерывают запуск без опубликованного результата.
  Плохое время / written считаются отдельно. Inferred calendar origin не нужен.

Полный syd01 (seed=0, key-share=1, 28.10.2026):

| Счётчик | Значение |
|---|---:|
| Все записи | 1 056 787 |
| GET blobs 200 / 206 | 211 523 / 15 |
| Events / versions | 211 538 / 12 587 |
| HEAD blobs | 247 497 |
| Manifests / uploads / pings | 471 294 / 72 895 / 53 563 |
| Wall `/usr/bin/time -l`, s | 49.32 |
| Peak RSS, MiB, getrusage (та же оговорка о sysctl, что выше) | 41.42 |

Лицензия самих данных на [странице DRTP](https://dssl.cs.vt.edu/drtp/) не указана.
Manifest сохраняет ровно `not stated; used with the authors' permission`.
Это статус разрешения, сообщённый автором задачи (письменное разрешение авторов);
конвертер не отправляет писем.
Открытая лицензия кода replayer не переносится на трассы.

```bash
# Из корня, новый каталог назначения:
/usr/bin/time -l build/trace-convert -source docker -zone syd01 \
  -input /Users/grishinid/home/98_Downloads/DockerRegistryTraces.tar.gz \
  -out build/docker-syd01
# Для частичной зоны, например dal09: -from 2017-07-01 -to 2017-07-08

GOMAXPROCS=2 GOMEMLIMIT=2GiB /usr/bin/time -l go run ./services/sppr/cmd/dataset \
  -catalog build/docker-syd01/catalog.json -events build/docker-syd01/events.jsonl \
  -start 2017-07-01T00:00:16.931Z -end 2017-09-03T23:59:54.126Z \
  -train-end 2017-08-15T00:00:16.931Z -validation-end 2017-08-25T00:00:16.931Z \
  -format csv.gz -train-every 15m -snapshot-every 15m -out build/docker-syd01-dataset
```

Dataset полного syd01 завершён после перезапуска: **41 178 981 строка**,
211 538 событий. Wall `/usr/bin/time -l` **562,27 s**, peak RSS через
getrusage **72,91 MiB**, exit code самого `go run` — 0. Запуск:
`GOMAXPROCS=2 GOMEMLIMIT=2GiB` (мягкая цель Go GC, не жёсткий предел RSS),
CSV статистика считалась параллельно отдельным процессом. Это ресурсное измерение конкретного прогона, не изолированный benchmark.

| Split | Строк | Известных labels | Нулевых labels | Доля нулевых среди известных |
|---|---:|---:|---:|---:|
| train | 19 616 197 | 19 616 197 | 19 408 124 | 98,9393% |
| validation | 9 855 332 | 9 855 332 | 9 755 245 | 98,9844% |
| test | 11 595 017 | 11 595 017 | 11 555 922 | 99,6628% |
| purged | 112 435 | 62 099 | 61 702 | 99,3607% |

Все train/validation/test строки eligible. Доля нулевых в этих трёх split
вместе — **99,1544%** (40 719 291 / 41 066 546). У 50 336 purged-строк
метка неизвестна: они не включены в знаменатель и не превращены в ноль.
Статистика получена полным потоковым чтением всех CSV, число строк сверено
с manifest; независимый расчёт по событиям, lifetime и сетке срезов совпал
для каждого split. Повторить подсчёт уже созданного CSV можно без обучения:

```bash
python3 - build/docker-syd01-dataset <<'PY_STATS'
import csv, gzip, sys
from pathlib import Path
root = Path(sys.argv[1])
for split in ('train', 'validation', 'test', 'purged'):
    rows = known = zero = 0
    with gzip.open(root / (split + '.csv.gz'), 'rt', newline='') as f:
        for row in csv.DictReader(f):
            rows += 1
            if row['label_reads'] != '':
                known += 1
                zero += int(row['label_reads'] == '0')
    print(split, 'rows=', rows, 'known=', known, 'zero=', zero,
          'zero_share=', zero / known if known else None)
PY_STATS
```

Нулевые labels означают отсутствие **наблюдённых выбранных успешных** GET blobs,
а не доказанный нулевой спрос во всей production-системе. Явные dataset bounds —
утверждение вызывающего о покрытии выбранного trace; filename range и последний
GET сами по себе не доказывают полноту. Данные test не используются для обучения.

## Проверки реализации

В `services/sppr`: `go vet ./...` прошёл. `go test ./...` запускается,
но в этой песочнице падает существующий `TestCloseReleasesListenerBeforeRun`:
`listen tcp 127.0.0.1:0: bind: operation not permitted`. Все остальные пакеты,
включая новые policy/streaming/CLI tests и старые Meta tests, проходят;
`go test ./... -skip '^TestCloseReleasesListenerBeforeRun$'` прошёл.
Тест открытия порта не изменялся. Полную команду без skip нужно повторить
на машине с разрешённым локальным bind.

Fixtures — ручные примеры: COS 21 строка, Docker 25 строк. Исходные архивы
и результаты в Git не входят. `README.md` и `GETTING_STARTED.md` не описывают
trace-convert и не требуют дополнительных изменений; runbook находится здесь
и в README SPPR. Учебные/экспериментальные результаты не являются доказательством
эффективности ML или tiering на production нагрузке.
