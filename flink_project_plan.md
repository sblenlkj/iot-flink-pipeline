# Flink IoT Project: план реализации и варианты jobs

Этот документ нужен как рабочая памятка по архитектуре проекта. Он не предназначен для сдачи как самостоятельный артефакт, но его можно использовать рядом с кодом и позже взять за основу для финального `README.md`.

## Общая идея проекта

Мы строим учебный streaming pipeline на Apache Flink:

```text
Python IoT generator → Kafka iot_events
Kafka source → Flink
Postgres manufacturers source → Flink
Flink join/window aggregation
Flink sink → Kafka output topics
Python consumer читает результат
```

Входные IoT-события содержат:

```text
sensor_id
manufacturer_id
event_time
temperature
humidity
```

Postgres хранит справочник производителей:

```text
id
manufacturer_name
country
description
```

Flink должен:

```text
1. Читать события из Kafka.
2. Читать справочник производителей из Postgres.
3. Делать join по manufacturer_id = id.
4. Считать оконные агрегаты по event time.
5. Писать результат обратно в Kafka.
```

Дополнительно по заданию нужно показать:

```text
- source и/или sink на SQL/Table API;
- переход между Table API и DataStream API.
```

## Почему делаем три варианта Flink job

Три варианта дадут не просто рабочий проект, а понятную демонстрацию разных execution styles во Flink.

```text
1. Основной production-like вариант:
   Table sources/join → DataStream window → ordinary Kafka

2. SQL/Table-only вариант:
   Table sources/join/window → upsert-kafka

3. Формальный bridge-вариант:
   Table sources/join → DataStream window → Table sink
```

На защите это можно объяснять так:

```text
Я сделал три job’а, чтобы показать разницу:
- SQL/Table API даёт changelog semantics и требует upsert sink.
- DataStream window даёт финальные append-only результаты окна.
- Flink позволяет смешивать Table API и DataStream API в одном pipeline.
```

## Вариант 1 — основной: Table join → DataStream window → обычная Kafka

Файл:

```text
src/iot_flink_pipeline/flink_jobs/table_join_to_datastream_kafka_job.py
```

Схема:

```text
Kafka Table source
Postgres Table source
        ↓
SQL/Table join
        ↓
Table → DataStream
        ↓
DataStream tumbling window
        ↓
обычный Kafka producer/sink
```

Это основной вариант, наиболее близкий к смыслу задания.

Что он показывает:

```text
1. Source Kafka реализован на SQL/Table API.
2. Source Postgres реализован на SQL/Table API.
3. Join сделан декларативно через SQL/Table API.
4. Переход Table → DataStream используется по делу.
5. Window сделан в DataStream API.
6. Sink Kafka обычный append-only: окно закрылось → отправили один результат.
```

Семантика результата:

```text
окно закрылось → отправили один финальный результат в Kafka
```

То есть consumer должен видеть не постоянные updates, а финальные сообщения окон:

```text
15:05-15:06 Bosch Sensortec count=37 avg_temp=22.1 avg_humidity=55.8
15:05-15:06 Sensirion       count=41 avg_temp=23.0 avg_humidity=57.2
15:05-15:06 Honeywell       count=33 avg_temp=21.8 avg_humidity=54.1
```

Этот вариант лучше всего объяснять преподавателю как основной:

```text
источники подняли как таблицы;
обогатили через SQL join;
перешли в DataStream;
сделали оконную обработку в event time;
записали результат в обычную Kafka.
```

## Вариант 2 — table-only: Table join/window → upsert-kafka

Файл:

```text
src/iot_flink_pipeline/flink_jobs/table_only_upsert_job.py
```

Схема:

```text
Kafka Table source
Postgres Table source
        ↓
SQL/Table join
        ↓
SQL/Table TUMBLE window
        ↓
upsert-kafka sink
```

Это демонстрация чистого SQL/Table API.

Семантика результата:

```text
пока окно живое, агрегат обновляется;
Kafka получает несколько версий одной и той же строки.
```

Пример того, что видит consumer:

```text
manufacturer_id=1, window=15:05-15:06, count=4
manufacturer_id=1, window=15:05-15:06, count=5
manufacturer_id=1, window=15:05-15:06, count=6
...
```

Почему нужен `upsert-kafka`:

```text
Table aggregation в streaming-режиме производит changelog/update stream,
а не простой append-only stream.
```

Обычный Kafka sink принимает только append-only поток. А table aggregation производит обновления агрегата. Поэтому обычный sink падает с ошибкой вида:

```text
Table sink doesn't support consuming update changes
```

`upsert-kafka` решает это через ключ:

```text
key = window_start + window_end + manufacturer_id
value = текущее значение агрегата
```

Правильный термин: **upsert-kafka**, не upstream Kafka.

```text
upsert = update + insert
```

## Вариант 3 — bridge-heavy: Table → DataStream window → Table sink

Файл:

```text
src/iot_flink_pipeline/flink_jobs/table_join_datastream_window_table_sink_job.py
```

Схема:

```text
Kafka Table source
Postgres Table source
        ↓
SQL/Table join
        ↓
Table → DataStream
        ↓
DataStream tumbling window
        ↓
DataStream → Table
        ↓
Table API Kafka sink
```

Это самый формальный bridge-вариант. Он немного искусственный, но хорошо закрывает формулировку задания:

```text
source на SQL/Table API;
переход между Table/DataStream API;
sink на SQL/Table API.
```

Смысл:

```text
Table API → DataStream API → Table API
```

Если DataStream window отдаёт финальные append-only результаты, то после возврата в Table можно писать в обычный Table Kafka sink, не в upsert-kafka.

Этот вариант полезен для демонстрации, что Flink позволяет собирать pipeline из разных API-представлений.

## Финальная структура Flink-адаптера

Предлагаемая структура:

```text
src/iot_flink_pipeline/flink_jobs/
├── __init__.py
│
├── common.py
│   └── Flink environment, jars, checkpointing, parallelism
│
├── ddl.py
│   └── Kafka/Postgres source tables, Kafka sink tables
│
├── joins.py
│   └── common Table API source registration + SQL join
│
├── datastream_windows.py
│   └── Table → DataStream + event-time window aggregation
│
├── datastream_sinks.py
│   └── DataStream → ordinary Kafka sink
│
├── table_join_to_datastream_kafka_job.py
│   └── main job: Table sources/join → DataStream window → Kafka
│
├── table_only_upsert_job.py
│   └── demo job: Table-only window → upsert-kafka
│
└── table_join_datastream_window_table_sink_job.py
    └── bridge job: Table → DataStream window → Table sink
```

Entrypoints:

```text
src/iot_flink_pipeline/entrypoints/
├── run_flink_datastream_window_job.py
├── run_flink_table_upsert_job.py
└── run_flink_bridge_table_sink_job.py
```

## Ответственность файлов

### `common.py`

Общее для всех трёх вариантов.

Содержит:

```text
- create_table_environment()
- настройку pipeline.jars
- настройку checkpoint interval
- настройку parallelism
```

То есть всё, что относится к созданию Flink runtime.

### `ddl.py`

Общее описание таблиц.

Содержит SQL DDL-строки:

```text
- create_iot_events_source_sql()
- create_manufacturers_source_sql()
- create_upsert_aggregates_sink_sql()
- create_append_aggregates_sink_sql()
```

Нужно два разных sink DDL:

```text
upsert sink:
    для table-only варианта

append kafka sink:
    для bridge-варианта, если после DataStream window возвращаем финальные append-only результаты
```

### `joins.py`

Общее для всех трёх вариантов: создание enriched table.

Содержит:

```text
- create_sources(t_env)
- create_enriched_events_view(t_env)
- get_enriched_events_table(t_env)
```

Логика enriched view:

```sql
CREATE TEMPORARY VIEW enriched_iot_events AS
SELECT
    e.sensor_id,
    e.manufacturer_id,
    m.manufacturer_name,
    m.country,
    e.event_ts,
    e.temperature,
    e.humidity
FROM iot_events e
JOIN manufacturers m
    ON e.manufacturer_id = m.id
WHERE
    e.sensor_id IS NOT NULL
    AND e.manufacturer_id IS NOT NULL
    AND e.temperature BETWEEN -50 AND 80
    AND e.humidity BETWEEN 0 AND 100
```

Так все три job’а стартуют с одинакового source + join, и это видно по коду.

### `datastream_windows.py`

Общее для вариантов 1 и 3.

Содержит:

```text
- create_window_result_stream(t_env, enriched_table)
```

Этот файл отвечает за:

```text
Table → DataStream
key_by
window
aggregate/process
```

На выходе — DataStream с финальными результатами окон.

Это главная обучающая часть для DataStream API.

### `datastream_sinks.py`

Нужно только для варианта 1.

Содержит:

```text
- sink_window_results_to_kafka(result_stream)
```

То есть запись DataStream напрямую в обычную Kafka.

## Job-файлы

### 1. `table_join_to_datastream_kafka_job.py`

Основной job.

Псевдокод:

```python
def run() -> None:
    t_env = create_table_environment()

    create_sources(t_env)
    create_enriched_events_view(t_env)

    enriched_table = t_env.from_path("enriched_iot_events")

    result_stream = create_window_result_stream(
        t_env=t_env,
        enriched_table=enriched_table,
    )

    sink_window_results_to_kafka(result_stream)
```

Смысл файла:

```text
table sources + join
→ datastream window
→ kafka sink
```

### 2. `table_only_upsert_job.py`

Table-only job.

Псевдокод:

```python
def run() -> None:
    t_env = create_table_environment()

    create_sources(t_env)
    create_enriched_events_view(t_env)
    create_upsert_sink(t_env)

    execute_table_window_upsert(t_env)
```

Здесь вся агрегация делается SQL’ем:

```sql
INSERT INTO iot_aggregates_upsert
SELECT ...
FROM TABLE(TUMBLE(...))
GROUP BY ...
```

### 3. `table_join_datastream_window_table_sink_job.py`

Полный bridge job.

Псевдокод:

```python
def run() -> None:
    t_env = create_table_environment()

    create_sources(t_env)
    create_enriched_events_view(t_env)

    enriched_table = t_env.from_path("enriched_iot_events")

    result_stream = create_window_result_stream(
        t_env=t_env,
        enriched_table=enriched_table,
    )

    result_table = create_window_result_table(
        t_env=t_env,
        result_stream=result_stream,
    )

    t_env.create_temporary_view("window_results", result_table)

    create_append_sink(t_env)
    insert_window_results_into_append_sink(t_env)
```

Смысл:

```text
Table → DataStream → Table → Kafka Table sink
```

## Предлагаемые Kafka topics

Чтобы не путаться, лучше сделать разные output topics:

```text
iot_events
iot_aggregates_upsert
iot_window_results
iot_window_results_table_sink
```

Consumer можно запускать явно:

```bash
uv run run-result-consumer --topic iot_aggregates_upsert
uv run run-result-consumer --topic iot_window_results
uv run run-result-consumer --topic iot_window_results_table_sink
```

Ожидаемое отличие:

```text
iot_aggregates_upsert:
    много обновлений одного окна

iot_window_results:
    финальные результаты закрытых окон

iot_window_results_table_sink:
    финальные результаты окон, но записанные через Table API sink
```

Для `iot_aggregates_upsert` можно включить compaction:

```text
cleanup.policy=compact
```

Обычные output-топики можно оставить стандартными.

## Как это объяснять на защите

Короткая версия:

```text
Я сделал три Flink job’а.

Первый — основной: source Kafka и source Postgres подняты через Table API, join сделан в SQL, затем результат переведён в DataStream, где сделано event-time tumbling window. После закрытия окна финальный результат пишется в обычный Kafka topic.

Второй — table-only: весь pipeline сделан через SQL/Table API. Так как streaming aggregation в Table API производит changelog/update stream, результат пишется в upsert-kafka.

Третий — bridge-вариант: source и sink сделаны через Table API, но window aggregation сделана в DataStream API. Это демонстрирует полный переход Table → DataStream → Table.
```

Главный вывод:

```text
Table API удобен для декларативных source, sink, join и SQL-аналитики.
DataStream API удобен для управляемой оконной и stateful-логики.
Один и тот же Flink pipeline можно собирать из обоих API.
```
