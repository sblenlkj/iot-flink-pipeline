# IoT Flink Pipeline. Сделал Дмитрий Каневский

Учебный проект по Apache Flink: Python-генератор создаёт IoT-события, Kafka хранит входной поток, Postgres хранит справочник производителей датчиков, а Flink считает оконные агрегаты по `event_time` и пишет результаты обратно в Kafka.

Проект специально содержит несколько Flink job, чтобы показать разные способы работы с Flink:

- **SQL/Table API** для Kafka/Postgres sources, SQL join и Table sinks.
- **DataStream API** для event-time оконной обработки.
- **Переходы между Table API и DataStream API**.
- **Pure DataStream job** со справочником производителей через broadcast state.

ATTENTION: весь код запускается через **uv** (пункты после №9 в настоящем README.md) - сверхбыстрый и современный менеджер пакетов и проектов для Python. Если Вы его не используете и хотите ограничиться стандартной библиотекой, следуйте инстукциям по запуску в `python_without_uv_appendix.md`.

---

## 1. Общая схема

```text
Python IoT generator
        │
        ▼
Kafka topic: iot_events
        │
        ▼
Apache Flink
        ├── Kafka source
        ├── Postgres source: manufacturers / sensors reference data
        ├── join по manufacturer_id
        ├── one-minute event-time tumbling windows
        └── Kafka sinks
                ├── iot_window_results
                ├── iot_aggregates_upsert
                ├── iot_window_results_table_sink
                └── iot_window_results_datastream

Python result consumer
        │
        ▼
печатает результаты из output topics
```

Входное IoT-событие:

```json
{
  "sensor_id": "sensor-001",
  "manufacturer_id": 1,
  "event_time": "2026-06-04T07:10:15.123456+00:00",
  "temperature": 22.4,
  "humidity": 55.1
}
```

Результат оконной агрегации:

```json
{
  "window_start": "2026-06-04 07:10:00.000",
  "window_end": "2026-06-04 07:11:00.000",
  "manufacturer_id": 1,
  "manufacturer_name": "Bosch Sensortec",
  "country": "Germany",
  "events_count": 20,
  "avg_temperature": 22.31,
  "avg_humidity": 54.92
}
```

---

## 2. Что реализовано

### 2.1. Генератор IoT-событий

Генератор:

- читает список сенсоров из Postgres;
- генерирует события батчами;
- пишет события в Kafka topic `iot_events`;
- добавляет небольшой jitter к `event_time`, чтобы порядок поступления сообщений и порядок event-time могли слегка отличаться.

Запуск:

```bash
uv run run-generator --events-per-batch 20
```

По умолчанию генератор работает бесконечно. Остановить можно через `Ctrl+C`.

### 2.2. Result consumer

Consumer слушает все result topics и печатает сообщения:

```bash
uv run run-result-consumer
```

Он подписывается на:

```text
iot_aggregates_upsert
iot_window_results
iot_window_results_table_sink
iot_window_results_datastream
```

В выводе видно, из какого topic пришло сообщение:

```text
[topic=iot_window_results partition=0 offset=12 topic_message_number=5] {...}
```

---

## 3. Flink jobs

Проект содержит четыре основных Flink job.

### 3.1. Main hybrid job: Table join → DataStream window → Kafka

Entrypoint:

```bash
uv run run-flink-datastream-window-job
```

Файл:

```text
src/iot_flink_pipeline/flink/jobs/table_join_to_datastream_kafka_job.py
```

Схема:

```text
Kafka Table source
Postgres Table source
        │
        ▼
SQL/Table API join
        │
        ▼
Table → DataStream
        │
        ▼
DataStream event-time tumbling window
        │
        ▼
ordinary Kafka sink: iot_window_results
```

Что демонстрирует:

- Kafka source на Table API;
- Postgres source на Table API;
- SQL join;
- переход `Table → DataStream`;
- оконную обработку в DataStream API;
- запись финальных append-only результатов в обычный Kafka topic.

Это основной вариант, наиболее близкий к формулировке задания.

---

### 3.2. Table-only job: SQL window → upsert-kafka

Entrypoint:

```bash
uv run run-flink-table-upsert-job
```

Файл:

```text
src/iot_flink_pipeline/flink/jobs/table_only_upsert_job.py
```

Схема:

```text
Kafka Table source
Postgres Table source
        │
        ▼
SQL/Table API TUMBLE window
        │
        ▼
SQL join with manufacturers
        │
        ▼
upsert-kafka sink: iot_aggregates_upsert
```

Почему используется `upsert-kafka`:

Streaming aggregation в Table API производит changelog/update stream, а не обычный append-only stream. Поэтому обычный Kafka Table sink не подходит для промежуточных обновлений агрегатов. `upsert-kafka` использует ключ:

```text
window_start + window_end + manufacturer_id
```

и хранит актуальную версию агрегата.

Ожидаемое поведение: в `iot_aggregates_upsert` могут появляться несколько версий результата для одного окна и одного производителя.

---

### 3.3. Bridge job: Table → DataStream window → Table sink

Entrypoint:

```bash
uv run run-flink-bridge-table-sink-job
```

Файл:

```text
src/iot_flink_pipeline/flink/jobs/table_join_datastream_window_table_sink_job.py
```

Схема:

```text
Kafka Table source
Postgres Table source
        │
        ▼
SQL/Table API join
        │
        ▼
Table → DataStream
        │
        ▼
DataStream event-time window
        │
        ▼
DataStream → Table
        │
        ▼
Table API Kafka sink: iot_window_results_table_sink
```

Этот вариант специально показывает полный bridge:

```text
Table API → DataStream API → Table API
```

Он более искусственный, чем основной job, но хорошо демонстрирует переход между API и Table API sink.

---

### 3.4. Pure DataStream job: KafkaSource → broadcast state → window → Kafka

Entrypoint:

```bash
uv run run-flink-pure-datastream-job
```

Файл:

```text
src/iot_flink_pipeline/flink/jobs/pure_datastream_job.py
```

Схема:

```text
KafkaSource
        │
        ▼
parse JSON
        │
        ▼
Postgres manufacturers snapshot
        │
        ▼
broadcast stream / broadcast state
        │
        ▼
connect event stream + broadcast stream
        │
        ▼
DataStream event-time window
        │
        ▼
Kafka sink: iot_window_results_datastream
```

Этот job не использует Table API для основной обработки. Он нужен для демонстрации DataStream API, connected streams и broadcast state.

---

## 4. Kafka topics

| Topic | Назначение |
|---|---|
| `iot_events` | входные IoT-события от Python generator |
| `iot_window_results` | основной hybrid job: Table join → DataStream window → Kafka |
| `iot_aggregates_upsert` | Table-only job с `upsert-kafka` |
| `iot_window_results_table_sink` | bridge job: Table → DataStream → Table sink |
| `iot_window_results_datastream` | pure DataStream job |
| `__consumer_offsets` | внутренний Kafka topic для consumer groups |

---

## 5. Event time, windows и watermarks

Проект использует **event time**, а не processing time.

### 5.1. Размер окна

Основное окно:

```python
TumblingEventTimeWindows.of(Time.minutes(1))
```

Окна имеют вид:

```text
11:04:00 - 11:05:00
11:05:00 - 11:06:00
11:06:00 - 11:07:00
```

Группировка идёт по:

```text
manufacturer_id
```

Для каждого производителя и каждой минуты считаются:

```text
events_count
avg_temperature
avg_humidity
```

### 5.2. Watermark в DataStream API

В DataStream-ветке `event_time` хранится как строка, затем переводится в epoch milliseconds:

```text
event_time string -> event_ts_ms long
```

После этого назначается watermark:

```python
WatermarkStrategy \
    .for_bounded_out_of_orderness(Duration.of_seconds(5)) \
    .with_timestamp_assigner(...)
```

Смысл:

```text
watermark = max_seen_event_time - 5 seconds
```

Окно закрывается, когда watermark прошёл конец окна:

```text
watermark >= window_end
```

### 5.3. Jitter в генераторе

Генератор добавляет небольшой случайный jitter к `event_time`:

```text
event_time = now + random jitter
```

Идея: события могут приходить в Kafka в одном порядке, но иметь немного другой логический `event_time`. Это делает watermark-политику осмысленной.

Jitter ограничен примерно диапазоном:

```text
[-4 sec, +4 sec]
```

А DataStream watermark допускает out-of-orderness:

```text
5 sec
```

То есть большинство слегка перемешанных событий всё ещё попадает в свои окна.

---

## 6. Важный фикс для Table API watermarks

Для Table-only job была обнаружена проблема: Kafka source читался, но SQL `TUMBLE` window не выпускал результат. Диагностика через `print` connector показала:

```text
SELECT из iot_events работает
TUMBLE window не печатает результат
```

Причина: topic `iot_events` имеет несколько partitions. Если одна partition временно idle, она может блокировать продвижение общего watermark. Без продвижения watermark event-time окно не закрывается.

Фикс находится в:

```text
src/iot_flink_pipeline/flink/common.py
```

Настройка:

```python
t_env.get_config().set("table.exec.source.idle-timeout", "10s")
```

Смысл:

```text
Если source partition не получает событий больше 10 секунд,
Flink считает её idle и не даёт ей блокировать общий watermark.
```

После этого Table-only job начал писать в `iot_aggregates_upsert`.

---

## 7. Структура проекта

Актуальная структура:

```text
.
├── README.md
├── docker-compose.yml
├── jars
│   ├── ...
├── pyproject.toml
├── scripts
│   └── run.sh
├── sql
│   └── postgres
│       ├── 001_create_iot_schema.sql
│       └── 002_seed_iot_reference_data.sql
├── src
│   └── iot_flink_pipeline
│       ├── domain
│       │   ├── events.py
│       │   └── sensors.py
│       ├── entrypoints
│       │   ├── run_flink_bridge_table_sink_job.py
│       │   ├── run_flink_datastream_window_job.py
│       │   ├── run_flink_pure_datastream_job.py
│       │   ├── run_flink_table_upsert_job.py
│       │   ├── run_generator.py
│       │   └── run_result_consumer.py
│       ├── flink
│       │   ├── common.py
│       │   ├── datastream_sinks.py
│       │   ├── datastream_window_table_bridge.py
│       │   ├── datastream_windows.py
│       │   ├── ddl.py
│       │   ├── joins.py
│       │   └── jobs
│       │       ├── pure_datastream_job.py
│       │       ├── table_join_datastream_window_table_sink_job.py
│       │       ├── table_join_to_datastream_kafka_job.py
│       │       └── table_only_upsert_job.py
│       ├── generator
│       │   └── event_factory.py
│       ├── kafka
│       │   ├── consumer.py
│       │   └── producer.py
│       ├── logging_config.py
│       └── settings.py
├── python_without_uv_appendix.md
└── uv.lock
```

---

## 8. Ответственность основных файлов

### `docker-compose.yml`

Поднимает инфраструктуру:

- Kafka;
- Kafka UI;
- Postgres;
- init container для Kafka topics.

### `sql/postgres`

SQL-скрипты для Postgres:

- `001_create_iot_schema.sql` — создаёт схему и таблицы;
- `002_seed_iot_reference_data.sql` — заполняет справочники производителей и сенсоров.

### `src/iot_flink_pipeline/domain`

Доменные структуры:

- `events.py` — IoT event;
- `sensors.py` — sensor specification.

### `src/iot_flink_pipeline/generator`

Генерация IoT-событий:

- выбор sensor;
- генерация температуры;
- генерация влажности;
- генерация `event_time` с jitter.

### `src/iot_flink_pipeline/kafka`

Python Kafka producer/consumer:

- `producer.py` — пишет события в `iot_events`;
- `consumer.py` — читает result topics.

### `src/iot_flink_pipeline/flink/common.py`

Создание Flink environments:

- DataStream environment;
- Table environment;
- подключение JAR-файлов;
- parallelism;
- checkpoint interval;
- `table.exec.source.idle-timeout`.

### `src/iot_flink_pipeline/flink/ddl.py`

SQL DDL для Flink Table API:

- Kafka source table `iot_events`;
- JDBC/Postgres source table `manufacturers`;
- upsert Kafka sink;
- append Kafka sink.

### `src/iot_flink_pipeline/flink/joins.py`

Общая Table API логика:

- регистрация source tables;
- создание `enriched_iot_events`;
- SQL join Kafka events + Postgres manufacturers.

### `src/iot_flink_pipeline/flink/datastream_windows.py`

Общая DataStream window-логика для основного job:

- `event_time` string → `event_ts_ms`;
- watermark strategy;
- `key_by(manufacturer_id)`;
- one-minute tumbling event-time window;
- JSON output.

### `src/iot_flink_pipeline/flink/datastream_window_table_bridge.py`

Bridge-логика для варианта `Table → DataStream → Table`:

- переиспользует DataStream window-подход;
- возвращает результат как Row stream;
- конвертирует DataStream обратно в Table.

### `src/iot_flink_pipeline/flink/datastream_sinks.py`

DataStream Kafka sink:

- создаёт Kafka sink для JSON strings;
- позволяет явно указать output topic.

### `src/iot_flink_pipeline/flink/jobs`

Конкретные Flink jobs:

- `table_join_to_datastream_kafka_job.py` — основной hybrid job;
- `table_only_upsert_job.py` — Table-only job;
- `table_join_datastream_window_table_sink_job.py` — bridge job;
- `pure_datastream_job.py` — pure DataStream + broadcast job.

### `src/iot_flink_pipeline/entrypoints`

Тонкие CLI entrypoints для запуска jobs и утилит.

---

## 9. Настройка окружения

### 9.1. Установка зависимостей

```bash
uv sync
```

### 9.2. `.env`

Создать `.env` из `.env.example`:

```bash
cp .env.example .env
```

Пример `.env.example`:

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=iot_flink
POSTGRES_USER=iot_user
POSTGRES_PASSWORD=iot_password

FLINK_PARALLELISM=1
FLINK_CHECKPOINT_INTERVAL=10s

KAFKA_CONNECTOR_JAR=jars/flink-sql-connector-kafka-4.0.1-2.0.jar
JDBC_CORE_JAR=jars/flink-connector-jdbc-core-4.0.0-2.0.jar
JDBC_POSTGRES_JAR=jars/flink-connector-jdbc-postgres-4.0.0-2.0.jar
POSTGRES_DRIVER_JAR=jars/postgresql-42.7.11.jar
```

### 9.3. JAR-файлы

JAR-файлы лежат в папке:

```text
jars/
```

Они не обязаны коммититься в репозиторий, но должны быть доступны локально по путям из `.env`.

---

## 10. Запуск инфраструктуры

```bash
docker compose up -d
```

Kafka UI:

```text
http://localhost:8080
```

Postgres:

```text
localhost:5432
```

Kafka bootstrap:

```text
localhost:9092
```

Остановить контейнеры без удаления данных:

```bash
docker compose stop
```

Полностью пересоздать инфраструктуру с удалением volumes:

```bash
docker compose down -v
docker compose up -d
```

---

## 11. Как запускать проект

Обычно нужно открыть несколько терминалов.

### Терминал 1: generator

```bash
uv run run-generator --events-per-batch 20
```

### Терминал 2: result consumer

```bash
uv run run-result-consumer
```

### Терминал 3: один из Flink jobs

Основной hybrid job:

```bash
uv run run-flink-datastream-window-job
```

Table-only upsert job:

```bash
uv run run-flink-table-upsert-job
```

Bridge job:

```bash
uv run run-flink-bridge-table-sink-job
```

Pure DataStream job:

```bash
uv run run-flink-pure-datastream-job
```

Можно также использовать helper script:

```bash
./scripts/run.sh run-flink-datastream-window-job
./scripts/run.sh run-flink-table-upsert-job
./scripts/run.sh run-flink-bridge-table-sink-job
./scripts/run.sh run-flink-pure-datastream-job
```

`run.sh` пишет лог job в файл вида:

```text
run-flink-datastream-window-job_YYYYMMDD_HHMMSS.log
```

---

## 12. Рекомендуемый clean run

Для чистой проверки удобно пересоздать topics через Kafka UI или полностью пересоздать compose:

```bash
docker compose down -v
docker compose up -d
```

Затем:

```bash
uv run run-result-consumer
uv run run-generator --events-per-batch 20
uv run run-flink-datastream-window-job
```

Подождать 1–2 минуты, чтобы event-time окна закрылись.

Ожидаемый результат:

```text
iot_events растёт
iot_window_results получает финальные результаты окон
run-result-consumer печатает JSON-сообщения
```

---

## 13. Почему после перезапуска job могут появляться дубли

Kafka source в Flink настроен на чтение с earliest offset:

```text
scan.startup.mode = earliest-offset
```

Это удобно для demo: job может прочитать уже существующие события из `iot_events`.

Но если output topic не очищать, при каждом новом запуске job старые входные события будут обработаны заново, и output topic снова получит результаты тех же окон.

Для разработки это нормально. Для чистой демонстрации лучше очищать/recreate topics перед запуском.

---

## 14. Очень краткое обьяснение

Короткое объяснение:

```text
Я сделал учебный IoT streaming pipeline на Flink.

Python generator пишет события в Kafka. Postgres хранит справочник производителей датчиков. Flink читает Kafka и Postgres, делает join, считает среднюю температуру и влажность в минутных event-time windows и пишет результат обратно в Kafka.

В проекте есть несколько jobs:
1. Table API sources + SQL join → DataStream window → Kafka sink.
2. Полностью Table API job с upsert-kafka.
3. Bridge Table → DataStream → Table sink.
4. Pure DataStream job с broadcast state для справочника производителей.

Так проект показывает и SQL/Table API, и DataStream API, и переходы между ними.
```

Про watermarks:

```text
Мы используем event time. Генератор добавляет небольшой jitter к event_time, поэтому события могут приходить немного не по порядку. DataStream job использует watermark strategy с bounded out-of-orderness 5 секунд. Окно закрывается, когда watermark проходит конец минуты.
```

Про Table API фикс:

```text
Для Table API окон над Kafka topic с несколькими partitions была добавлена настройка table.exec.source.idle-timeout = 10s. Без неё idle partition могла блокировать watermark, и SQL TUMBLE window не выпускал результат.
```

---

## 15. Текущий статус

Рабочие компоненты:

- Docker infrastructure;
- Postgres schema and seed data;
- Kafka topics;
- Python generator;
- Python result consumer;
- main hybrid Flink job;
- Table-only upsert Flink job;
- bridge Table/DataStream/Table job;
- pure DataStream job with broadcast state.

