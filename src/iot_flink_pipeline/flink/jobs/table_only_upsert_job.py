import logging

from pyflink.table import StreamTableEnvironment

from iot_flink_pipeline.flink.common import create_table_environment
from iot_flink_pipeline.flink.ddl import create_upsert_aggregates_sink_sql
from iot_flink_pipeline.flink.joins import create_sources

logger = logging.getLogger(__name__)


def create_upsert_sink(t_env: StreamTableEnvironment) -> None:
    t_env.execute_sql(create_upsert_aggregates_sink_sql())


def run_table_window_upsert(t_env: StreamTableEnvironment) -> None:
    result = t_env.execute_sql(
        """
        INSERT INTO iot_aggregates_upsert
        SELECT
            CAST(a.window_start AS STRING) AS window_start,
            CAST(a.window_end AS STRING) AS window_end,
            a.manufacturer_id,
            m.manufacturer_name,
            m.country,
            a.events_count,
            a.avg_temperature,
            a.avg_humidity
        FROM (
            SELECT
                window_start,
                window_end,
                manufacturer_id,
                COUNT(*) AS events_count,
                ROUND(AVG(temperature), 2) AS avg_temperature,
                ROUND(AVG(humidity), 2) AS avg_humidity
            FROM TABLE(
                TUMBLE(
                    TABLE iot_events,
                    DESCRIPTOR(event_ts),
                    INTERVAL '1' MINUTE
                )
            )
            WHERE
                sensor_id IS NOT NULL
                AND manufacturer_id IS NOT NULL
                AND event_ts IS NOT NULL
                AND temperature IS NOT NULL
                AND humidity IS NOT NULL
                AND temperature BETWEEN -50.0 AND 80.0
                AND humidity BETWEEN 0.0 AND 100.0
            GROUP BY
                window_start,
                window_end,
                manufacturer_id
        ) AS a
        JOIN manufacturers AS m
            ON a.manufacturer_id = m.id
        """
    )

    result.wait()


def run() -> None:
    logger.info(
        "Starting job: Table source/window -> Table join -> upsert-kafka sink"
    )

    t_env = create_table_environment()

    create_sources(t_env)
    create_upsert_sink(t_env)
    run_table_window_upsert(t_env)