import logging

from pyflink.table import StreamTableEnvironment

from iot_flink_pipeline.flink.common import create_table_environment
from iot_flink_pipeline.flink.ddl import create_upsert_aggregates_sink_sql
from iot_flink_pipeline.flink.joins import create_sources

logger = logging.getLogger(__name__)


def create_upsert_sink(t_env: StreamTableEnvironment) -> None:
    """Register upsert-kafka sink table for Table API results."""
    t_env.execute_sql(create_upsert_aggregates_sink_sql())


def run_table_window_upsert(t_env: StreamTableEnvironment) -> None:
    """Run pure Table API join, event-time window aggregation, and upsert Kafka sink."""
    result = t_env.execute_sql(
        """
        INSERT INTO iot_aggregates_upsert
        SELECT
            CAST(window_start AS STRING) AS window_start,
            CAST(window_end AS STRING) AS window_end,
            m.id AS manufacturer_id,
            m.manufacturer_name,
            m.country,
            COUNT(*) AS events_count,
            ROUND(AVG(e.temperature), 2) AS avg_temperature,
            ROUND(AVG(e.humidity), 2) AS avg_humidity
        FROM TABLE(
            TUMBLE(
                TABLE iot_events,
                DESCRIPTOR(event_ts),
                INTERVAL '1' MINUTE
            )
        ) AS e
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
        GROUP BY
            window_start,
            window_end,
            m.id,
            m.manufacturer_name,
            m.country
        """
    )

    result.wait()


def run() -> None:
    """Run Table-only job: Kafka source + Postgres join + SQL window + upsert Kafka."""
    logger.info(
        "Starting job: Table-only Kafka source -> SQL window/join -> upsert-kafka sink"
    )

    t_env = create_table_environment()

    create_sources(t_env)
    create_upsert_sink(t_env)
    run_table_window_upsert(t_env)