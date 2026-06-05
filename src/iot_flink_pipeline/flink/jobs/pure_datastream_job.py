import json
import logging
from datetime import datetime, timezone
from typing import Any, Iterable, cast

import psycopg
from pyflink.common import Duration, Row, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import (
    TimestampAssigner,
    WatermarkStrategy,
)
from pyflink.datastream import (
    BroadcastStream,
    DataStream,
    StreamExecutionEnvironment,
)
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.functions import BroadcastProcessFunction
from pyflink.datastream.state import MapStateDescriptor

from iot_flink_pipeline.flink.common import create_stream_environment
from iot_flink_pipeline.flink.datastream_sinks import (
    sink_json_stream_to_kafka,
)
from iot_flink_pipeline.flink.datastream_windows import (
    create_window_result_json_stream_from_enriched_stream,
)
from iot_flink_pipeline.settings import settings

logger = logging.getLogger(__name__)


MANUFACTURERS_STATE_DESCRIPTOR = MapStateDescriptor(
    "manufacturers-broadcast-state",
    Types.SHORT(),
    Types.ROW_NAMED(
        ["id", "manufacturer_name", "country"],
        [Types.SHORT(), Types.STRING(), Types.STRING()],
    ),
)


def _parse_event_time_to_utc_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def _parse_event_time_to_epoch_millis(value: str) -> int:
    return int(_parse_event_time_to_utc_datetime(value).timestamp() * 1000)


class IoTEventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(
        self,
        value: Row,
        record_timestamp: int,
    ) -> int:
        row = cast(Any, value)
        return int(row.event_ts_ms)


def load_manufacturers_from_postgres() -> list[Row]:
    query = """
        SELECT id, manufacturer_name, country
        FROM iot.manufacturers
        ORDER BY id;
    """

    with psycopg.connect(settings.postgres_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

    manufacturers = [
        Row(
            id=int(manufacturer_id),
            manufacturer_name=str(manufacturer_name),
            country=str(country),
        )
        for manufacturer_id, manufacturer_name, country in rows
    ]

    if not manufacturers:
        raise RuntimeError(
            "No manufacturers found in Postgres table iot.manufacturers"
        )

    return manufacturers


def create_kafka_source() -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(settings.kafka_bootstrap_servers)
        .set_topics(settings.iot_events_topic)
        .set_group_id("iot-pure-datastream-events-reader")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def parse_event(value: str) -> Row:
    payload = json.loads(value)

    event_time = str(payload["event_time"])
    event_ts_ms = _parse_event_time_to_epoch_millis(event_time)

    return Row(
        sensor_id=str(payload["sensor_id"]),
        manufacturer_id=int(payload["manufacturer_id"]),
        event_time=event_time,
        event_ts_ms=event_ts_ms,
        temperature=float(payload["temperature"]),
        humidity=float(payload["humidity"]),
    )


def create_event_stream(env: StreamExecutionEnvironment) -> DataStream:
    kafka_source = create_kafka_source()

    raw_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "iot-events-kafka-source",
    )

    parsed_stream = raw_stream.map(
        parse_event,
        output_type=Types.ROW_NAMED(
            [
                "sensor_id",
                "manufacturer_id",
                "event_time",
                "event_ts_ms",
                "temperature",
                "humidity",
            ],
            [
                Types.STRING(),
                Types.SHORT(),
                Types.STRING(),
                Types.LONG(),
                Types.DOUBLE(),
                Types.DOUBLE(),
            ],
        ),
    )

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(IoTEventTimestampAssigner())
    )

    return parsed_stream.assign_timestamps_and_watermarks(watermark_strategy)


def create_manufacturers_broadcast_stream(
    env: StreamExecutionEnvironment,
) -> BroadcastStream:
    manufacturers = load_manufacturers_from_postgres()
    logger.info("Loaded %s manufacturers from Postgres", len(manufacturers))

    manufacturers_stream = env.from_collection(
        manufacturers,
        type_info=Types.ROW_NAMED(
            ["id", "manufacturer_name", "country"],
            [Types.SHORT(), Types.STRING(), Types.STRING()],
        ),
    )

    return manufacturers_stream.broadcast(MANUFACTURERS_STATE_DESCRIPTOR)


class EnrichWithManufacturersFunction(BroadcastProcessFunction):
    def process_broadcast_element(
        self,
        value: Row,
        ctx: BroadcastProcessFunction.Context,
    ) -> Iterable[Row]:
        state = ctx.get_broadcast_state(MANUFACTURERS_STATE_DESCRIPTOR)
        row = cast(Any, value)
        state.put(int(row.id), value)
        return []

    def process_element(
        self,
        value: Row,
        ctx: BroadcastProcessFunction.ReadOnlyContext,
    ) -> Iterable[Row]:
        state = ctx.get_broadcast_state(MANUFACTURERS_STATE_DESCRIPTOR)

        event = cast(Any, value)
        manufacturer = state.get(int(event.manufacturer_id))

        if manufacturer is None:
            return []

        manufacturer_row = cast(Any, manufacturer)

        return [
            Row(
                sensor_id=event.sensor_id,
                manufacturer_id=int(event.manufacturer_id),
                manufacturer_name=manufacturer_row.manufacturer_name,
                country=manufacturer_row.country,
                event_time=event.event_time,
                event_ts_ms=int(event.event_ts_ms),
                temperature=float(event.temperature),
                humidity=float(event.humidity),
            )
        ]


def create_enriched_stream(env: StreamExecutionEnvironment) -> DataStream:
    event_stream = create_event_stream(env)
    manufacturers_broadcast_stream = create_manufacturers_broadcast_stream(env)

    return event_stream.connect(manufacturers_broadcast_stream).process(
        EnrichWithManufacturersFunction(),
        output_type=Types.ROW_NAMED(
            [
                "sensor_id",
                "manufacturer_id",
                "manufacturer_name",
                "country",
                "event_time",
                "event_ts_ms",
                "temperature",
                "humidity",
            ],
            [
                Types.STRING(),
                Types.SHORT(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.LONG(),
                Types.DOUBLE(),
                Types.DOUBLE(),
            ],
        ),
    )


def run() -> None:
    logger.info(
        "Starting job: pure DataStream Kafka -> broadcast PG ref -> event-time window -> Kafka"
    )

    env = create_stream_environment()

    enriched_stream = create_enriched_stream(env)

    result_stream = create_window_result_json_stream_from_enriched_stream(
        enriched_stream
    )

    sink_json_stream_to_kafka(
        result_stream,
        topic=settings.iot_window_results_datastream_topic,
        sink_name="pure-datastream-window-kafka-sink",
    )

    env.execute("pure-datastream-event-time-window-kafka-job")