import logging

from iot_flink_pipeline.flink.common import (
    create_stream_and_table_environment,
)
from iot_flink_pipeline.flink.datastream_sinks import (
    sink_json_stream_to_kafka,
)
from iot_flink_pipeline.flink.datastream_windows import (
    create_window_result_json_stream_from_table,
)
from iot_flink_pipeline.flink.joins import (
    create_sources_and_enriched_view,
)

logger = logging.getLogger(__name__)


def run() -> None:
    logger.info(
        "Starting job: Table sources/join -> DataStream event-time window -> Kafka sink"
    )

    env, t_env = create_stream_and_table_environment()

    create_sources_and_enriched_view(t_env)

    enriched_table = t_env.from_path("enriched_iot_events")

    result_stream = create_window_result_json_stream_from_table(
        t_env=t_env,
        enriched_table=enriched_table,
    )

    sink_json_stream_to_kafka(result_stream)

    env.execute("table-join-to-datastream-event-time-window-kafka-job")