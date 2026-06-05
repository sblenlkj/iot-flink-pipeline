from iot_flink_pipeline.settings import settings


def create_iot_events_source_sql() -> str:
    return f"""
    CREATE TABLE iot_events (
        sensor_id STRING,
        manufacturer_id SMALLINT,
        event_time STRING,
        temperature DOUBLE,
        humidity DOUBLE,

        event_ts AS TO_TIMESTAMP(
            REPLACE(SUBSTRING(event_time, 1, 23), 'T', ' ')
        ),
        WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{settings.iot_events_topic}',
        'properties.bootstrap.servers' = '{settings.kafka_bootstrap_servers}',
        'properties.group.id' = 'iot-flink-events-reader',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    )
    """


def create_manufacturers_source_sql() -> str:
    return f"""
    CREATE TABLE manufacturers (
        id SMALLINT,
        manufacturer_name STRING,
        country STRING,
        description STRING,
        PRIMARY KEY (id) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = '{settings.postgres_jdbc_url}',
        'table-name' = 'iot.manufacturers',
        'username' = '{settings.postgres_user}',
        'password' = '{settings.postgres_password}',
        'driver' = 'org.postgresql.Driver'
    )
    """


def create_upsert_aggregates_sink_sql() -> str:
    return f"""
    CREATE TABLE iot_aggregates_upsert (
        window_start STRING,
        window_end STRING,
        manufacturer_id SMALLINT,
        manufacturer_name STRING,
        country STRING,
        events_count BIGINT,
        avg_temperature DOUBLE,
        median_humidity DOUBLE,
        PRIMARY KEY (window_start, window_end, manufacturer_id) NOT ENFORCED
    ) WITH (
        'connector' = 'upsert-kafka',
        'topic' = '{settings.iot_aggregates_upsert_topic}',
        'properties.bootstrap.servers' = '{settings.kafka_bootstrap_servers}',
        'key.format' = 'json',
        'value.format' = 'json',
        'value.fields-include' = 'ALL'
    )
    """


def create_append_aggregates_sink_sql() -> str:
    return f"""
    CREATE TABLE iot_window_results_table_sink (
        window_start STRING,
        window_end STRING,
        manufacturer_id SMALLINT,
        manufacturer_name STRING,
        country STRING,
        events_count BIGINT,
        avg_temperature DOUBLE,
        median_humidity DOUBLE
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{settings.iot_window_results_table_sink_topic}',
        'properties.bootstrap.servers' = '{settings.kafka_bootstrap_servers}',
        'format' = 'json'
    )
    """