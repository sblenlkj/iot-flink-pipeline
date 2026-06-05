# Appendix: запуск проекта без `uv`

Этот проект удобно запускать через `uv`, но он не является обязательным. Ниже приведён вариант запуска через обычный Python `venv` и `pip`.

## 1. Создать виртуальное окружение

Из корня проекта:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
```

Проверить версию Python:

```bash
python --version
```

Ожидается Python 3.11.x.

## 2. Обновить инструменты установки

```bash
python -m pip install --upgrade pip setuptools wheel
```

`setuptools` важен, потому что некоторые зависимости PyFlink / Beam могут ожидать `pkg_resources`.

## 3. Установить проект и зависимости

Если проект описан в `pyproject.toml`, можно установить его в editable-режиме:

```bash
pip install -e .
```

Если при установке `apache-beam` появится ошибка про `pkg_resources`, повторить:

```bash
pip install --upgrade setuptools
pip install -e .
```

## 4. Подготовить `.env`

Скопировать пример конфигурации:

```bash
cp .env.example .env
```

В `.env` должны быть указаны пути к JAR-файлам Flink connectors. Пути должны быть абсолютными или корректными `file://` URI, например:

```env
KAFKA_CONNECTOR_JAR=file:///absolute/path/to/jars/flink-sql-connector-kafka-4.0.1-2.0.jar
JDBC_CORE_JAR=file:///absolute/path/to/jars/flink-connector-jdbc-core-4.0.0-2.0.jar
JDBC_POSTGRES_JAR=file:///absolute/path/to/jars/flink-connector-jdbc-postgres-4.0.0-2.0.jar
POSTGRES_DRIVER_JAR=file:///absolute/path/to/jars/postgresql-42.7.11.jar
```

Остальные параметры Kafka/Postgres обычно можно оставить как в `.env.example`, если используется стандартный `docker-compose.yml` из проекта.

## 5. Запустить инфраструктуру

```bash
docker compose up -d
```

Проверить контейнеры:

```bash
docker compose ps
```

Kafka UI обычно доступен по адресу:

```text
http://localhost:8080
```

## 6. Запуск entrypoints без `uv`

После `pip install -e .` console scripts из `pyproject.toml` доступны напрямую внутри активированного `.venv`.

Генератор IoT-событий:

```bash
run-generator --events-per-batch 20
```

Consumer результатов:

```bash
run-result-consumer
```

Основной Flink job:

```bash
run-flink-datastream-window-job
```

Table-only upsert job:

```bash
run-flink-table-upsert-job
```

Bridge Table → DataStream → Table job:

```bash
run-flink-bridge-table-sink-job
```

Pure DataStream job:

```bash
run-flink-pure-datastream-job
```

## 7. Альтернативный запуск через `python -m`

Если console scripts по какой-то причине недоступны, можно запускать entrypoint-модули напрямую:

```bash
python -m iot_flink_pipeline.entrypoints.run_generator
python -m iot_flink_pipeline.entrypoints.run_result_consumer
python -m iot_flink_pipeline.entrypoints.run_flink_datastream_window_job
python -m iot_flink_pipeline.entrypoints.run_flink_table_upsert_job
python -m iot_flink_pipeline.entrypoints.run_flink_bridge_table_sink_job
python -m iot_flink_pipeline.entrypoints.run_flink_pure_datastream_job
```

Для генератора с параметром:

```bash
python -m iot_flink_pipeline.entrypoints.run_generator --events-per-batch 20
```

## 8. Рекомендуемый порядок проверки

Открыть несколько терминалов из корня проекта, в каждом активировать окружение:

```bash
source .venv/bin/activate
```

Дальше:

```bash
# terminal 1
run-result-consumer

# terminal 2
run-flink-datastream-window-job

# terminal 3
run-generator --events-per-batch 20
```

Через 1–2 минуты consumer должен начать печатать результаты окон из Kafka output topic.

## 9. Очистка после работы

Остановить контейнеры без удаления данных:

```bash
docker compose stop
```

Остановить и удалить контейнеры, но оставить volumes:

```bash
docker compose down
```

Полностью сбросить Kafka/Postgres volumes:

```bash
docker compose down -v
```

Полный сброс удобен перед чистым demo-запуском, но он удалит данные Kafka topics и Postgres volume.
