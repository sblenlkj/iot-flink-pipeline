import logging

from pyflink.table import StreamTableEnvironment

from iot_flink_pipeline.flink.common import create_table_environment
from iot_flink_pipeline.flink.datastream_window_table_bridge import (
    create_window_result_row_stream_from_table,
    create_window_result_table,
)
from iot_flink_pipeline.flink.ddl import create_append_aggregates_sink_sql
from iot_flink_pipeline.flink.joins import (
    create_sources_and_enriched_view,
)

logger = logging.getLogger(__name__)


def create_append_sink(t_env: StreamTableEnvironment) -> None:
    """Register regular Kafka Table API sink for final window results."""
    t_env.execute_sql(create_append_aggregates_sink_sql())


def insert_window_results_into_table_sink(t_env: StreamTableEnvironment) -> None:
    """Insert final append-only window results into Kafka Table sink."""
    result = t_env.execute_sql(
        """
        INSERT INTO iot_window_results_table_sink
        SELECT
            window_start,
            window_end,
            manufacturer_id,
            manufacturer_name,
            country,
            events_count,
            avg_temperature,
            avg_humidity
        FROM window_results
        """
    )

    result.wait()


def run() -> None:
    """Run bridge job: Table join -> DataStream window -> Table Kafka sink."""
    logger.info(
        "Starting job: Table sources/join -> DataStream event-time window -> Table Kafka sink"
    )

    t_env = create_table_environment()

    create_sources_and_enriched_view(t_env)

    enriched_table = t_env.from_path("enriched_iot_events")

    result_stream = create_window_result_row_stream_from_table(
        t_env=t_env,
        enriched_table=enriched_table,
    )

    result_table = create_window_result_table(
        t_env=t_env,
        result_stream=result_stream,
    )

    t_env.create_temporary_view("window_results", result_table)

    create_append_sink(t_env)
    insert_window_results_into_table_sink(t_env)