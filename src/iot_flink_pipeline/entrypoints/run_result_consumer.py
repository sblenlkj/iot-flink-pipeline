import logging

from iot_flink_pipeline.logging_config import setup_logging
from iot_flink_pipeline.kafka.consumer import KafkaJsonPrinterConsumer
from iot_flink_pipeline.settings import settings


RESULT_TOPICS: tuple[str, ...] = (
    settings.iot_aggregates_upsert_topic,
    settings.iot_window_results_topic,
    settings.iot_window_results_table_sink_topic,
    settings.iot_window_results_datastream_topic,
)


def main() -> None:
    setup_logging()

    consumer = KafkaJsonPrinterConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topics=RESULT_TOPICS,
        group_id="iot-result-printer",
    )

    logging.info("Result consumer started")
    logging.info("Listening result topics: %s", ", ".join(RESULT_TOPICS))

    for message in consumer.consume_forever():
        print(
            f"[topic={message.topic} "
            f"partition={message.partition} "
            f"offset={message.offset} "
            f"topic_message_number={message.topic_message_number}] "
            f"{message.value}"
        )


if __name__ == "__main__":
    main()