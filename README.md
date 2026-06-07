# IoT Flink Pipeline. Сделал Дмитрий Каневский

Учебный проект по Apache Flink: Python-генератор создаёт IoT-события, Kafka хранит входной поток, Postgres хранит справочники производителей и сенсоров, а Flink читает данные, обогащает события справочником, считает оконные агрегаты по `event_time` и пишет результаты обратно в Kafka.

Проект специально содержит четыре варианта Flink job, чтобы показать разные режимы работы Flink на одной и той же задаче:

- **Table-only job** — полностью решает задачу через SQL/Table API: Kafka source, Postgres source, SQL `JOIN`, SQL `TUMBLE` window и `upsert-kafka` sink. Этот вариант пишет changelog/update-сообщения, потому что streaming aggregation в Table API обновляет агрегат по мере прихода новых событий.

- **Pure DataStream job** — полностью решает задачу через DataStream API: `KafkaSource`, broadcast state для справочника производителей, event-time window и обычный Kafka sink. Этот вариант пишет в Kafka только финальные результаты закрытых окон.

- **Table → DataStream → Kafka job** — использует Table API для Kafka/Postgres sources и SQL join, затем переходит в DataStream API для оконной обработки. Этот вариант тоже пишет в Kafka только финальные результаты закрытых окон.

- **Table → DataStream → Table job** — демонстрирует полный bridge между API: Table API sources/join, DataStream window, возврат обратно в Table API и запись через Table Kafka sink. Несмотря на возврат в Table API, результат уже сформирован DataStream-окном, поэтому в Kafka также пишутся только финальные результаты закрытых окон.

Главное отличие: **Table-only job** показывает changelog/upsert-семантику Table API, а остальные три job используют DataStream event-time window и поэтому отправляют в Kafka уже готовые финальные агрегаты по закрытым окнам.

Основной результат проекта: все четыре Flink job работают и пишут результаты в отдельные Kafka topics.

---

## 1. Что делает проект

Входные IoT-события имеют поля:

```json
{
  "sensor_id": "sensor-001",
  "manufacturer_id": 1,
  "event_time": "2026-06-05T18:13:42.123456+00:00",
  "temperature": 22.4,
  "humidity": 55.1
}
```

Postgres хранит справочник производителей:

```text
id
manufacturer_name
country
description
```

Flink должен:

1. читать IoT-события из Kafka topic `iot_events`;
2. читать справочник производителей из Postgres;
3. делать join по `manufacturer_id`;
4. считать оконные агрегаты в минутных event-time windows;
5. сохранять результат в Kafka.

Финальный результат окна:

```json
{
  "window_start": "2026-06-05 18:13:00.000",
  "window_end": "2026-06-05 18:14:00.000",
  "manufacturer_id": 1,
  "manufacturer_name": "Bosch Sensortec",
  "country": "Germany",
  "events_count": 20,
  "avg_temperature": 22.31,
  "median_humidity": 54.92
}
```

Считаются:

```text
events_count
avg_temperature
median_humidity
```

То есть по заданию используется **средняя температура** и **медиана влажности**.

---

## 2. Общая схема

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

---

## 3. Быстрый старт

Проект можно запускать двумя способами:

- через `uv`;
- через обычный Python `venv` + `pip`.

Ниже оба варианта идут параллельно.

### 3.1. Подготовить JAR-файлы Flink connectors

Flink Kafka/JDBC connectors и PostgreSQL JDBC driver нужны PyFlink job'ам для работы с Kafka и Postgres.

Скачать JAR-файлы:

```bash
curl -L -o jars/flink-sql-connector-kafka-4.0.1-2.0.jar \
  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar

curl -L -o jars/flink-connector-jdbc-core-4.0.0-2.0.jar \
  https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc-core/4.0.0-2.0/flink-connector-jdbc-core-4.0.0-2.0.jar

curl -L -o jars/flink-connector-jdbc-postgres-4.0.0-2.0.jar \
  https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc-postgres/4.0.0-2.0/flink-connector-jdbc-postgres-4.0.0-2.0.jar

curl -L -o jars/postgresql-42.7.11.jar \
  https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.11/postgresql-42.7.11.jar
```

Проверить:

```bash
ls -lh jars
file jars/*.jar
jar tf jars/flink-sql-connector-kafka-4.0.1-2.0.jar | head
jar tf jars/postgresql-42.7.11.jar | head
```

JAR-файлы можно не коммитить в GitHub, но они должны лежать локально по путям из `.env`.

### 3.2. Создать `.env`

```bash
cp .env.example .env
```

Пример `.env`:

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=iot_flink
POSTGRES_USER=iot_user
POSTGRES_PASSWORD=iot_password

FLINK_PARALLELISM=1
FLINK_CHECKPOINT_INTERVAL=10s

WATERMARK_DELAY=10

KAFKA_CONNECTOR_JAR=jars/flink-sql-connector-kafka-4.0.1-2.0.jar
JDBC_CORE_JAR=jars/flink-connector-jdbc-core-4.0.0-2.0.jar
JDBC_POSTGRES_JAR=jars/flink-connector-jdbc-postgres-4.0.0-2.0.jar
POSTGRES_DRIVER_JAR=jars/postgresql-42.7.11.jar
```

`WATERMARK_DELAY=10` — общий параметр задержки watermark в секундах. Он используется и в DataStream API, и в Table API DDL.

### 3.3. Установить зависимости

Вариант через `uv`:

```bash
uv sync
```

Вариант через обычный Python:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
pip install -e .
```

Если при установке `apache-beam` появится ошибка про `pkg_resources`, повторить:

```bash
pip install --upgrade setuptools
pip install -e .
```

### 3.4. Запустить инфраструктуру

```bash
docker compose up -d
```

Проверить контейнеры:

```bash
docker compose ps
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

## 4. Запуск: параллельный режим

Обычно нужно открыть несколько терминалов.

### Терминал 1: result consumer

Consumer подписывается на все result topics и позволяет уведить в терминале, что все работает, без необходимости открытия Kafka UI в браузере или исполнения docker команд.

Через `uv`:

```bash
uv run run-result-consumer
```

Через обычный Python после `source .venv/bin/activate`:

```bash
run-result-consumer
```

Или через модуль:

```bash
python -m iot_flink_pipeline.entrypoints.run_result_consumer
```

### Терминал 2: generator

По дефолту будет 10 событий в батче. Это число можно изменить с помощью параметра `--events-per-batch`.

Через `uv`:

```bash
uv run run-generator --events-per-batch 20
```

Через обычный Python:

```bash
run-generator --events-per-batch 20
```

Или через модуль:

```bash
python -m iot_flink_pipeline.entrypoints.run_generator --events-per-batch 20
```

### Терминал 3: один из Flink jobs

Основной hybrid job:

```bash
uv run run-flink-datastream-window-job
# или без uv:
run-flink-datastream-window-job
# или:
python -m iot_flink_pipeline.entrypoints.run_flink_datastream_window_job
```

Table-only upsert job:

```bash
uv run run-flink-table-upsert-job
# или без uv:
run-flink-table-upsert-job
# или:
python -m iot_flink_pipeline.entrypoints.run_flink_table_upsert_job
```

Bridge job:

```bash
uv run run-flink-bridge-table-sink-job
# или без uv:
run-flink-bridge-table-sink-job
# или:
python -m iot_flink_pipeline.entrypoints.run_flink_bridge_table_sink_job
```

Pure DataStream job:

```bash
uv run run-flink-pure-datastream-job
# или без uv:
run-flink-pure-datastream-job
# или:
python -m iot_flink_pipeline.entrypoints.run_flink_pure_datastream_job
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

Это не засоряет терминал в результать `Ctrl+C`. 

---

## 5. Запуск: demo-режим по очереди

Для демонстрации необязательно держать generator и все jobs параллельно. Удобный сценарий:

1. Запустить infrastructure:

```bash
docker compose up -d
```

2. Запустить generator на 2–3 минуты:

```bash
uv run run-generator --events-per-batch 20
# или без uv:
run-generator --events-per-batch 20
```

3. Остановить generator через `Ctrl+C`.

4. Запустить result consumer:

```bash
uv run run-result-consumer
# или без uv:
run-result-consumer
```

5. По очереди запускать Flink jobs:

```bash
uv run run-flink-datastream-window-job
uv run run-flink-table-upsert-job
uv run run-flink-bridge-table-sink-job
uv run run-flink-pure-datastream-job
```

или без `uv`:

```bash
run-flink-datastream-window-job
run-flink-table-upsert-job
run-flink-bridge-table-sink-job
run-flink-pure-datastream-job
```

Почему это работает:

```text
Flink jobs читают iot_events с earliest offset,
поэтому они могут обработать события, которые уже лежат в Kafka.
```

Такой режим удобен для защиты: можно один раз накопить входные данные и показать, как разные jobs обрабатывают один и тот же набор событий.

---

## 6. Flink jobs

В проекте четыре основных Flink job.

### 6.1. Main hybrid job: Table join → DataStream window → Kafka

Entrypoint:

```bash
run-flink-datastream-window-job
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

### 6.2. Table-only job: SQL window → upsert-kafka

Entrypoint:

```bash
run-flink-table-upsert-job
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

Ожидаемое поведение: в `iot_aggregates_upsert` может быть больше сообщений, чем во входном `iot_events`, потому что Table API пишет не только финальные значения окна, а changelog/update-сообщения.

### 6.3. Bridge job: Table → DataStream window → Table sink

Entrypoint:

```bash
run-flink-bridge-table-sink-job
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

Этот вариант показывает полный bridge:

```text
Table API → DataStream API → Table API
```

Он более искусственный, чем основной job, но хорошо демонстрирует переход между Datastream API и Table API sink.

### 6.4. Pure DataStream job: KafkaSource → broadcast state → window → Kafka

Entrypoint:

```bash
run-flink-pure-datastream-job
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

## 7. Kafka topics

| Topic | Назначение |
|---|---|
| `iot_events` | входные IoT-события от Python generator |
| `iot_window_results` | основной hybrid job: Table join → DataStream window → Kafka |
| `iot_aggregates_upsert` | Table-only job с `upsert-kafka` |
| `iot_window_results_table_sink` | bridge job: Table → DataStream → Table sink |
| `iot_window_results_datastream` | pure DataStream job |
| `__consumer_offsets` | внутренний Kafka topic для consumer groups |

---

## 8. Event time, windows и watermarks

Проект использует **event time**, а не processing time.

### 8.1. Размер окна

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
median_humidity
```

### 8.2. Jitter в генераторе

Генератор добавляет небольшой случайный jitter к `event_time`:

```text
event_time = now + random jitter
```

Jitter ограничен примерно диапазоном:

```text
[-4 sec, +4 sec]
```

Идея: события могут приходить в Kafka в одном порядке, но иметь немного другой логический `event_time`. Это делает watermark-политику осмысленной.

### 8.3. Почему watermark delay выбран 10 секунд

Важный момент: если jitter равен `[-4 sec, +4 sec]`, то два события, сгенерированные почти одновременно, могут отличаться по `event_time` примерно на 8 секунд.

Пример:

```text
processing/send time ≈ 01:03

event A: event_time = 01:07
event B: event_time = 00:59
```

Если event A с `event_time = 01:07` придёт во Flink раньше, то при watermark delay 5 секунд:

```text
max_seen_event_time = 01:07
watermark = 01:07 - 5 sec = 01:02
```

Окно:

```text
00:00 - 01:00
```

уже закроется, потому что:

```text
watermark 01:02 >= window_end 01:00
```

Если после этого придёт event B с `event_time = 00:59`, он будет late event для уже закрытого окна.

Поэтому watermark delay выбран с запасом:

```text
WATERMARK_DELAY=10
```

Логика такая:

```text
jitter range: [-4 sec, +4 sec]
maximum event-time spread inside batch: about 8 sec
watermark delay: 10 sec
```

Это уменьшает риск late events для нашей модели генерации.

Компромисс:

```text
чем больше watermark delay,
тем меньше late events,
но тем позже Flink выпускает результат окна.
```

### 8.4. Watermark в DataStream API

В DataStream-ветке `event_time` хранится как строка, затем переводится в epoch milliseconds:

```text
event_time string -> event_ts_ms long
```

После этого назначается watermark:

```python
WatermarkStrategy \
    .for_bounded_out_of_orderness(Duration.of_seconds(settings.watermark_delay)) \
    .with_timestamp_assigner(...)
```

Смысл:

```text
watermark = max_seen_event_time - settings.watermark_delay seconds
```

Окно закрывается, когда watermark прошёл конец окна:

```text
watermark >= window_end
```

### 8.5. Watermark в Table API

В Table API source для Kafka создаётся computed column:

```sql
event_ts AS TO_TIMESTAMP(...)
```

и watermark:

```sql
WATERMARK FOR event_ts AS event_ts - INTERVAL {settings.watermark_delay} SECOND
```

Это соответствует лекционному подходу: строковое время события превращается в timestamp, а затем для него объявляется watermark.

---

## 9. Важный фикс для Table API watermarks

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

### Примечание: почему Table API jobs могут дописать больше окон после паузы

После добавления настройки:

```python
t_env.get_config().set("table.exec.source.idle-timeout", "10s")
```

Table API jobs начинают корректно продвигать watermark даже тогда, когда часть Kafka partitions больше не получает события.

Это влияет на поведение после остановки generator:

- **DataStream-only job** может продолжать ждать новые события, потому что последние окна закрываются только после продвижения watermark новыми событиями.
- **Table API jobs** через `source.idle-timeout` через некоторое время считают неактивные Kafka partitions idle. После этого они могут продвинуть watermark и выпустить ещё часть оконных результатов.

Поэтому при одинаковом входном topic `iot_events` количество сообщений в output topics может немного отличаться:

```text
iot_window_results_datastream     -> может выпустить меньше финальных окон, если generator уже остановлен
iot_window_results                -> может выпустить больше окон после idle-timeout
iot_window_results_table_sink     -> аналогично может дописать окна после idle-timeout
```

Это не ошибка. Это следствие разной политики продвижения watermark:

```text
DataStream window ждёт продвижения event-time watermark новыми событиями.
Table API source с idle-timeout умеет перестать учитывать idle partitions и поэтому может закрыть окна после паузы.
```

В live-режиме, когда generator продолжает писать события, различия обычно менее заметны: новые события сами продвигают watermark, и окна постепенно закрываются во всех вариантах.

---

## 10. Структура проекта

Актуальная структура:

```text
.
├── README.md
├── docker-compose.yml
├── jars
│   ├── .gitkeep
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
├── tree.txt
└── uv.lock
```

---

## 11. Ответственность основных файлов

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

## 14. Почему upsert topic может содержать больше сообщений, чем input topic

`iot_aggregates_upsert` — это не обычный append-only topic с финальными результатами. Table-only streaming aggregation производит changelog/update stream.

Поэтому для одного окна и одного производителя могут появляться несколько версий:

```text
events_count = 1
events_count = 2
events_count = 3
...
```

Из-за этого `iot_aggregates_upsert` может содержать больше сообщений, чем `iot_events`. Это ожидаемое поведение для `upsert-kafka`.

---

## 15. Troubleshooting

### 15.1. Producer warning: Coordinator load in progress

Иногда при старте Kafka producer можно увидеть warning:

```text
Failed to acquire idempotence PID from broker localhost:9092/1:
Broker: Coordinator load in progress: retrying
```

Если после этого generator продолжает писать:

```text
Produced batch: batch_size=5, total_produced=10
```

значит всё нормально. Broker coordinator ещё догружался, producer повторил запрос и продолжил работу.

### 15.2. Table API job читает Kafka, но не пишет оконные результаты

Если `SELECT` из Kafka source работает, но `TUMBLE` window не выпускает результат, проверьте настройку:

```python
t_env.get_config().set("table.exec.source.idle-timeout", "10s")
```

Она нужна, чтобы idle Kafka partition не блокировала watermark.

### 15.3. Output topic растёт при каждом перезапуске job

Это ожидаемо, потому что source читает с `earliest-offset`. Для чистого запуска очистите output topics или пересоздайте Docker volumes:

```bash
docker compose down -v
docker compose up -d
```

---

## 16. Краткое объяснение проекта

```text
Я сделал учебный IoT streaming pipeline на Flink.

Python generator пишет события в Kafka. Postgres хранит справочник производителей датчиков. Flink читает Kafka и Postgres, делает join, считает среднюю температуру и медиану влажности в минутных event-time windows и пишет результат обратно в Kafka.

В проекте есть несколько jobs:
1. Table API sources + SQL join → DataStream window → Kafka sink.
2. Полностью Table API job с upsert-kafka.
3. Bridge Table → DataStream → Table sink.
4. Pure DataStream job с broadcast state для справочника производителей.

Так проект показывает и SQL/Table API, и DataStream API, и переходы между ними.
```

Про watermarks:

```text
Мы используем event time. Генератор добавляет jitter к event_time в диапазоне примерно [-4s, +4s]. Это значит, что два события из одного batch могут отличаться по event_time примерно на 8 секунд. Поэтому watermark delay вынесен в настройку WATERMARK_DELAY и выбран равным 10 секундам. Окно закрывается, когда watermark проходит конец минуты.
```

Про Table API фикс:

```text
Для Table API окон над Kafka topic с несколькими partitions была добавлена настройка table.exec.source.idle-timeout = 10s. Без неё idle partition могла блокировать watermark, и SQL TUMBLE window не выпускал результат.
```

---

## 17. Текущий статус

Рабочие компоненты:

- Docker infrastructure;
- Postgres schema and seed data;
- Kafka topics;
- Python generator;
- Python result consumer;
- main hybrid Flink job;
- Table-only upsert Flink job;
- bridge Table/DataStream/Table job;
- pure DataStream job with broadcast state;
- event-time windows;
- watermarks;
- average temperature;
- median humidity.
