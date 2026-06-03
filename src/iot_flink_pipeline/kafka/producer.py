import logging
from typing import Any

from confluent_kafka import Producer

from iot_flink_pipeline.domain.events import IoTEvent

logger = logging.getLogger(__name__)


class IoTKafkaProducer:
    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self._topic = topic
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
                "client.id": "iot-event-generator",
            }
        )

    def produce(self, event: IoTEvent) -> None:
        self._producer.produce(
            topic=self._topic,
            key=event.kafka_key,
            value=event.to_kafka_value(),
            on_delivery=self._on_delivery,
        )

    def poll(self) -> None:
        self._producer.poll(0)

    def flush(self) -> None:
        self._producer.flush()

    @staticmethod
    def _on_delivery(error: Any, message: Any, *, log_delivery: bool = False) -> None:
        if error is not None:
            logger.error("Failed to deliver message: %s", error)
            return

        if log_delivery:
            logger.info(
                "Message delivered to %s [%s] offset=%s",
                message.topic(),
                message.partition(),
                message.offset(),
            )