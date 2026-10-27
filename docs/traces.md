# Трассы: аудит доступа и преобразование

Проверено 28.10.2026 до написания конвертеров. Использованы страницы описаний,
S3 ListObjectsV2, HEAD и Range: 256 КиБ Meta, до 16 КиБ ответа Google Drive.
Полные трассы не скачивались; аккаунты не создавались, условия не принимались.

| Источник | Доступ и размер | Формат и поля | Лицензия/условия | Статус |
|---|---|---|---|---|
| Meta CDN, март 2023 | Публичный [S3 CacheMon](https://cache-datasets.s3.amazonaws.com/?list-type=2&prefix=cache_dataset_txt/2023_metaCDN/), без регистрации; три файла ниже | zstd CSV с заголовком: timestamp (Unix ms), cacheKey, OpType, objectSize, responseSize, responseHeaderSize, rangeStart/End, TTL, SamplingRate, cache_hit, item_value, RequestHandler, cdn_content_type_id, vip_type | [Оригинал CacheLib](https://cachelib.org/docs/Cache_Library_User_Guides/Cachebench_FB_HW_eval/) разрешает свободное скачивание под лицензией CacheLib ([Apache-2.0](https://github.com/facebook/CacheLib/blob/main/LICENSE)); [коллекция CacheMon](https://github.com/cacheMon/cache_dataset#license) объявляет CC BY 4.0, требует attribution, ссылку и описание изменений | Доступна; конвертер ниже. HTTP status отсутствует — успех GET не доказуем напрямую |
| IBM Cloud Object Storage (презентация SNIA 2021) | [SNIA IOTTA trace set 36305](https://iotta.snia.org/traces/key-value?only=36305), также [указатель /tracetypes/17](https://iotta.snia.org/tracetypes/17). По [презентации IBM](https://www.snia.org/sites/default/files/SDCEMEA/2021/snia-evaluating-cache-performance-using-cloud-storage-traces.pdf), слайды 17–18: 98 трасс, 88 GB, 1,6 млрд запросов, одна неделя 2019; в индексированной странице IOTTA: trace 000 — 731 MB, 001 — 26,6 MB, 002 — 3,78 MB (размеры объявления, не HEAD) | По презентации: относительное время ms, GET/PUT/HEAD/DELETE OBJECT, object ID, object size, необязательные offsets; точный разделитель и файл не проверены | [Индексированная страница IOTTA](https://iotta.snia.org/traces/key-value/36305) указывает [SNIA Trace Data Files Download License](https://iotta.snia.org/repository/download_license) и необходимость cookies; текст лицензии, диалог согласия и регистрация не проверены из-за `Verifying That You Are Human` | Остановлено на проверке человека; конвертера нет |
| IBM Docker Registry, FAST 2018 | [Проект DRTP](https://dssl.cs.vt.edu/drtp/) → [Google Drive](https://drive.google.com/file/d/1FjgyCJ8YayOKeMlxtbznhsiXGPYC9yP8/view): DockerRegistryTraces.tar.gz, 1 662 385 619 байт (метаданные viewer), показано 1.5G; [статья](https://people.cs.vt.edu/~butta/docs/fast2018-dockerAnalysis.pdf): 22,4 GB обработанных трасс, 75 дней, 7 зон | Архив tar.gz; согласно статье JSON-записи: host, http.request.duration/method/remoteaddr/uri/useragent, http.response.status/written, id, timestamp UTC. Заголовок архива не получен | Лицензия самих данных на странице проекта не указана; лицензия replayer не заменяет лицензию трасс. Drive требует `Download anyway` (virus scan warning) | Остановлено перед подтверждением скачивания; конвертера нет |

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

Что нужно автору для заблокированных источников:

1. IBM COS: открыть ссылку trace set 36305 в обычном браузере с JavaScript и
   cookies, пройти проверку человека; открыть описание и Download. Если появятся
   регистрация, login или соглашение — выполнить их самостоятельно. Прочитать [SNIA Trace Data Files Download License](https://iotta.snia.org/repository/download_license);
   если Download требует согласия — принять его самостоятельно. Сохранить
   README/лицензию, список файлов с размерами и выбранный файл локально. Наличие
   регистрации/отдельного диалога согласия пока неизвестно.
2. IBM Docker: открыть Drive по ссылке выше, самому решить подтверждение
   `Download anyway`; до использования выяснить разрешение/лицензию данных у
   авторов проекта (контакты на DRTP). Передать локальный архив и условия.
   Login при просмотре не потребовался; скачивание за подтверждением не проверялось.
