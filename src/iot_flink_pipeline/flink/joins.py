from pyflink.table import StreamTableEnvironment

from iot_flink_pipeline.flink.ddl import (
    create_iot_events_source_sql,
    create_manufacturers_source_sql,
)


def create_sources(t_env: StreamTableEnvironment) -> None:
    t_env.execute_sql(create_iot_events_source_sql())
    t_env.execute_sql(create_manufacturers_source_sql())


def create_enriched_events_view(t_env: StreamTableEnvironment) -> None:
    t_env.execute_sql(
        """
        CREATE TEMPORARY VIEW enriched_iot_events AS
        SELECT
            e.sensor_id,
            e.manufacturer_id,
            m.manufacturer_name,
            m.country,
            e.event_ts,
            e.temperature,
            e.humidity
        FROM iot_events AS e
        JOIN manufacturers AS m
            ON e.manufacturer_id = m.id
        WHERE
            e.sensor_id IS NOT NULL
            AND e.manufacturer_id IS NOT NULL
            AND e.event_ts IS NOT NULL
            AND e.temperature IS NOT NULL
            AND e.humidity IS NOT NULL
            AND e.temperature BETWEEN -50.0 AND 80.0
            AND e.humidity BETWEEN 0.0 AND 100.0
        """
    )


def create_sources_and_enriched_view(t_env: StreamTableEnvironment) -> None:
    create_sources(t_env)
    create_enriched_events_view(t_env)