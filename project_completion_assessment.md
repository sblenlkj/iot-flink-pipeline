# Оценка выполненности проекта IoT Flink Pipeline

## Краткий вывод

Проект можно считать готовым к отправке преподавателю и защите. Минимальные требования задания закрыты полностью, а часть требований выполнена с запасом: реализовано несколько Flink jobs, показаны разные режимы работы с Table API и DataStream API, добавлена работа в event time, watermarks, Kafka/Postgres sources, Kafka sinks, а также отдельный pure DataStream-вариант с broadcast state.

Итоговая оценка готовности: **высокая**.

Ожидаемая оценка по 10-балльной шкале: **9–10/10**, если преподаватель принимает замену абстрактного “типа устройства” из задания на справочную категорию “производитель устройства”. Чтобы снять этот риск, на защите стоит явно сказать: в проекте производитель играет роль справочной категории устройства из Postgres.

---

## 1. Соответствие требованиям задания

### 1.1. Генератор IoT-сообщений

Требование:

```text
Написать генератор сообщений от IoT устройств:
- тип устройства / id
- время события
- температура
- влажность
публиковать события в Kafka
```

Что реализовано:

```text
Python generator
→ читает sensor specs из Postgres
→ генерирует sensor_id, manufacturer_id, event_time, temperature, humidity
→ пишет события батчами в Kafka topic iot_events
→ добавляет controlled jitter к event_time для демонстрации event-time / watermark
```

Статус: **выполнено полностью**.

---

### 1.2. DDL/DML для Postgres-справочника

Требование:

```text
Разработать DDL/DML скрипты для создания и наполнения справочника типов IoT устройств
```

Что реализовано:

```text
sql/postgres/001_create_iot_schema.sql
sql/postgres/002_seed_iot_reference_data.sql
```

В Postgres созданы и заполнены справочники:

```text
manufacturers
sensors
```

Статус: **выполнено полностью**.

Проект даже немного богаче минимального требования, потому что есть не только справочник производителей, но и отдельный справочник сенсоров.

---

### 1.3. Kafka source во Flink

Требование:

```text
src: kafka
```

Что реализовано:

```text
Table API Kafka source для iot_events
DataStream KafkaSource для pure DataStream job
```

Статус: **выполнено полностью**.

---

### 1.4. Postgres source во Flink

Требование:

```text
src: pg
```

Что реализовано:

```text
JDBC/Postgres Table source manufacturers
Postgres snapshot для pure DataStream broadcast state
```

Статус: **выполнено полностью**.

---

### 1.5. Join Kafka events + Postgres reference

Требование:

```text
Соединяем события из Kafka со статичным справочником из Postgres
```

Что реализовано:

```text
SQL join в Table API вариантах
broadcast-state enrichment в pure DataStream варианте
```

Статус: **выполнено полностью и с запасом**.

Проект показывает два разных подхода к enrichment:

```text
Table API / SQL JOIN
DataStream API / broadcast state
```

---

### 1.6. Работа в event time

Требование:

```text
Flink, работа в event time
```

Что реализовано:

```text
event_time приходит в Kafka-событии
Table API: computed event_ts + WATERMARK FOR event_ts
DataStream API: timestamp assigner + WatermarkStrategy
```

Дополнительно введена единая настройка:

```text
watermark_delay = 10 seconds
```

Статус: **выполнено полностью**.

Watermark delay выбран осознанно: генератор добавляет jitter к event_time примерно в диапазоне `[-4s, +4s]`, значит два события, пришедшие рядом по processing time, могут отличаться по event time примерно на 8 секунд. Watermark delay 10 секунд даёт запас и снижает вероятность late events.

---

### 1.7. Window aggregation

Требование:

```text
Рассчитываем среднюю температуру и медиану влажности в каждой минуте
```

Что реализовано:

```text
1-minute tumbling event-time windows
events_count
avg_temperature
median_humidity
```

Группировка выполняется по:

```text
window_start
window_end
manufacturer_id / manufacturer_name
```

Статус: **выполнено полностью**.

Комментарий: в оригинальной формулировке задания требуется выводить “тип устройства из pg”. В проекте роль справочной категории выполняет производитель устройства. Поэтому агрегация по производителю является корректным аналогом агрегации по типу устройства.

---

### 1.8. Kafka sink

Требование:

```text
sink: kafka
сохраняем результат в topic Kafka
```

Что реализовано:

```text
iot_window_results
iot_aggregates_upsert
iot_window_results_table_sink
iot_window_results_datastream
```

Статус: **выполнено полностью**.

---

### 1.9. Source/sink на SQL/Table API

Требование:

```text
Реализовать источник и/или получатель на SQL/Table API
```

Что реализовано:

```text
Kafka Table source
Postgres JDBC Table source
upsert-kafka Table sink
regular Kafka Table sink
```

Статус: **выполнено полностью и с запасом**.

---

### 1.10. Переход между DataStream API и SQL/Table API

Требование:

```text
Реализовать переход между DataStream и SQL/Table API
```

Что реализовано:

```text
Table → DataStream → Kafka
Table → DataStream → Table → Kafka
```

Статус: **выполнено полностью**.

Особенно сильный вариант — `Table → DataStream → Table`, потому что он демонстрирует полный bridge между API туда и обратно.

---

## 2. Реализованные Flink jobs

В проекте есть четыре рабочих варианта обработки одной и той же задачи.

### 2.1. Table-only job

```text
Kafka Table source
Postgres Table source
SQL JOIN
SQL TUMBLE window
upsert-kafka sink
```

Этот вариант полностью решает задачу через SQL/Table API. Он пишет changelog/update-сообщения, потому что streaming aggregation в Table API обновляет агрегат по мере прихода новых событий.

Output topic:

```text
iot_aggregates_upsert
```

---

### 2.2. Pure DataStream job

```text
KafkaSource
Postgres manufacturers snapshot
broadcast state
DataStream event-time window
Kafka sink
```

Этот вариант полностью решает задачу через DataStream API и показывает работу с broadcast state для справочника производителей.

Output topic:

```text
iot_window_results_datastream
```

Этот job пишет в Kafka только финальные результаты закрытых окон.

---

### 2.3. Table → DataStream → Kafka job

```text
Kafka Table source
Postgres Table source
SQL JOIN
Table → DataStream
DataStream event-time window
Kafka sink
```

Это основной hybrid-вариант, наиболее близкий к формулировке задания: source реализованы на Table API, join сделан через SQL, оконная обработка сделана в DataStream API.

Output topic:

```text
iot_window_results
```

Этот job пишет в Kafka только финальные результаты закрытых окон.

---

### 2.4. Table → DataStream → Table job

```text
Kafka Table source
Postgres Table source
SQL JOIN
Table → DataStream
DataStream event-time window
DataStream → Table
Table Kafka sink
```

Этот вариант демонстрирует полный bridge между API: Table API → DataStream API → Table API.

Output topic:

```text
iot_window_results_table_sink
```

Несмотря на возврат в Table API, результат уже сформирован DataStream-окном, поэтому в Kafka также пишутся только финальные результаты закрытых окон.

---

## 3. Сильные стороны проекта

Проект закрывает требования не минимально, а с запасом.

Сильные стороны:

```text
- четыре разные Flink job для одной задачи;
- Table API Kafka/Postgres sources;
- SQL JOIN;
- SQL TUMBLE window;
- DataStream event-time window;
- bridge Table → DataStream;
- bridge Table → DataStream → Table;
- pure DataStream job;
- broadcast state для справочника производителей;
- upsert-kafka sink для Table aggregation;
- обычные Kafka sinks для финальных результатов окон;
- controlled event_time jitter в генераторе;
- watermarks и объяснимая политика watermark_delay = 10s;
- фикс idle Kafka partitions через table.exec.source.idle-timeout;
- медиана влажности;
- Kafka UI для проверки;
- result consumer для просмотра output topics;
- запуск через uv и через обычный Python venv/pip;
- русскоязычный README.
```

---

## 4. Важные технические решения

### 4.1. Почему используется производитель вместо типа устройства

В задании требуется справочник типов IoT-устройств из Postgres. В проекте вместо абстрактного типа используется справочник производителей датчиков.

Это корректная адаптация домена:

```text
manufacturer_id в событии
→ join с Postgres manufacturers
→ manufacturer_name в результате
```

То есть производитель играет роль справочной категории из Postgres, аналогичной “типу устройства” из задания.

---

### 4.2. Почему watermark delay равен 10 секунд

Генератор добавляет jitter к `event_time` примерно в диапазоне:

```text
[-4s, +4s]
```

Это значит, что два события, пришедшие рядом по processing time, могут отличаться по event time примерно на 8 секунд.

Пример:

```text
событие A: event_time = 01:07
событие B: event_time = 00:59
```

Если событие `01:07` пришло первым, watermark при задержке 5 секунд стал бы:

```text
01:07 - 5s = 01:02
```

Окно `00:00–01:00` уже могло бы закрыться, и событие `00:59` стало бы late.

Поэтому выбран watermark delay:

```text
10 seconds
```

Это даёт запас относительно максимального разброса jitter и снижает вероятность late events.

---

### 4.3. Почему нужен `table.exec.source.idle-timeout`

В процессе отладки была найдена проблема: Table API Kafka source читал события, но SQL `TUMBLE` window не выпускал результат.

Причина: topic `iot_events` имеет несколько partitions. Если одна partition не получает события, она может блокировать общий watermark. Без продвижения watermark окно не закрывается.

Фикс:

```python
t_env.get_config().set("table.exec.source.idle-timeout", "10s")
```

Смысл:

```text
Если Kafka partition не получает события больше 10 секунд,
Flink считает её idle и перестаёт учитывать при расчёте общего watermark.
```

После этого Table-only job начал корректно писать результаты.

---

### 4.4. Почему Table-only job пишет больше сообщений

`iot_aggregates_upsert` — это upsert/changelog topic.

Table API streaming aggregation может выдавать промежуточные обновления агрегата:

```text
events_count = 1
events_count = 2
events_count = 3
...
```

Поэтому сообщений в `iot_aggregates_upsert` может быть больше, чем во входном `iot_events`.

Это ожидаемое поведение.

---

## 5. Возможные вопросы преподавателя и короткие ответы

### Почему не `TypeName`, а `manufacturer_name`?

В задании нужен справочник типов IoT-устройств из Postgres. В проекте вместо абстрактного типа устройства используется справочник производителей датчиков. Семантически это та же роль: событие содержит `manufacturer_id`, Flink делает join с Postgres и выводит справочное поле `manufacturer_name`.

### Почему группировка по производителю, а не по `sensor_id`?

Потому что задание требует вывести справочную категорию из Postgres. `sensor_id` — это конкретный источник события, а `manufacturer_id/manufacturer_name` — справочная категория, полученная через join с Postgres.

### Почему есть `upsert-kafka`?

Потому что Table API streaming aggregation создаёт changelog/update stream. Обычный Kafka Table sink ожидает append-only поток, а `upsert-kafka` умеет обновлять логическую строку по ключу.

### Почему три других job пишут меньше сообщений?

Потому что они считают окна в DataStream API и отправляют только финальный результат закрытого окна. Table-only job пишет changelog/update-сообщения.

### Почему watermark 10 секунд?

Потому что generator добавляет jitter к `event_time` примерно `[-4s, +4s]`. Максимальный разброс между событиями может быть около 8 секунд, поэтому 10 секунд — разумный запас.

### Почему нужен `table.exec.source.idle-timeout`?

Потому что idle Kafka partition может блокировать общий watermark в Table API. `source.idle-timeout` позволяет Flink не учитывать временно неактивные partitions и закрывать event-time окна.

---

## 6. Рекомендуемый финальный demo-checklist

Перед отправкой или защитой можно выполнить чистый прогон:

```bash
docker compose down -v
docker compose up -d
```

Затем сгенерировать входные события:

```bash
uv run run-generator --events-per-batch 20
```

Подержать generator 2–3 минуты и остановить через `Ctrl+C`.

После этого по очереди запустить jobs:

```bash
uv run run-flink-datastream-window-job
uv run run-flink-table-upsert-job
uv run run-flink-bridge-table-sink-job
uv run run-flink-pure-datastream-job
```

Проверить Kafka UI:

```text
http://localhost:8080
```

Ожидаемые topics:

```text
iot_events
iot_window_results
iot_aggregates_upsert
iot_window_results_table_sink
iot_window_results_datastream
```

Также можно запустить consumer:

```bash
uv run run-result-consumer
```

Он должен печатать JSON-сообщения с результатами окон.

---

## 7. Итоговая оценка готовности

```text
Минимальные требования задания: закрыты полностью.
Требование про SQL/Table API source/sink: закрыто полностью.
Требование про переход между DataStream и SQL/Table API: закрыто полностью.
Event time / watermarks: реализовано и объяснено.
Postgres DDL/DML: есть.
Kafka source/sink: есть.
Документация: есть.
Проект готов к отправке.
```

Итог:

```text
Готовность проекта: высокая.
Риск перед защитой: низкий.
Ожидаемая оценка: 9–10/10.
```

Основной риск: преподаватель может спросить, почему вместо “типа устройства” используется производитель. Ответ: производитель в проекте играет роль справочной категории устройства из Postgres, а технически задача решена тем же способом: id в событии → join со справочником → агрегация по справочной категории.
