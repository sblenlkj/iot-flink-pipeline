from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import DataStream
from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaRecordSerializationSchema,
    KafkaSink,
)

from iot_flink_pipeline.settings import settings


def create_json_kafka_sink(*, topic: str) -> KafkaSink:
    """Create Kafka sink for JSON string DataStream results."""
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(settings.kafka_bootstrap_servers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )


def sink_json_stream_to_kafka(
    result_stream: DataStream,
    *,
    topic: str,
    sink_name: str,
) -> None:
    """Write JSON string DataStream results to selected Kafka topic."""
    result_stream.sink_to(
        create_json_kafka_sink(topic=topic)
    ).name(sink_name)