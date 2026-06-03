from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import DataStream
from pyflink.datastream.connectors.kafka import (
    KafkaRecordSerializationSchema,
    KafkaSink,
)

from iot_flink_pipeline.settings import settings


def create_plain_kafka_sink() -> KafkaSink:
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(settings.kafka_bootstrap_servers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(settings.iot_window_results_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )


def sink_json_stream_to_kafka(result_stream: DataStream) -> None:
    result_stream.sink_to(create_plain_kafka_sink()).name(
        "iot-window-results-kafka-sink"
    )