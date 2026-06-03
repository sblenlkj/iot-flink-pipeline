import logging
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass

from confluent_kafka import Consumer, KafkaException

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class KafkaConsumedMessage:
    topic: str
    value: str
    topic_message_number: int
    partition: int | None = None
    offset: int | None = None


class KafkaJsonPrinterConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        topics: tuple[str, ...],
        group_id: str = "iot-result-printer",
        auto_offset_reset: str = "earliest",
    ) -> None:
        if not topics:
            raise ValueError("At least one Kafka topic must be provided")

        self._topics = topics
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": auto_offset_reset,
                "enable.auto.commit": True,
            }
        )

        self._counters: dict[str, int] = defaultdict(int)
        self._seen_topics: set[str] = set()

    def consume_forever(self) -> Iterator[KafkaConsumedMessage]:
        logger.info("Subscribing to Kafka topics: %s", ", ".join(self._topics))
        self._consumer.subscribe(list(self._topics))

        try:
            while True:
                message = self._consumer.poll(timeout=1.0)

                if message is None:
                    continue

                if message.error():
                    raise KafkaException(message.error())

                value = message.value()
                if value is None:
                    continue

                topic = message.topic()
                if topic is None:
                    raise KafkaException("Topic is None")

                if topic not in self._seen_topics:
                    self._seen_topics.add(topic)
                    logger.info("Started receiving messages from topic: %s", topic)

                self._counters[topic] += 1

                yield KafkaConsumedMessage(
                    topic=topic,
                    partition=message.partition(),
                    offset=message.offset(),
                    value=value.decode("utf-8"),
                    topic_message_number=self._counters[topic],
                )

        finally:
            logger.info("Closing Kafka consumer")
            self._consumer.close()