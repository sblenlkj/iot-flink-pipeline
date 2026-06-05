import argparse
import logging
import random
import time

import psycopg

from iot_flink_pipeline.domain.sensors import SensorSpec
from iot_flink_pipeline.generator.event_factory import IoTEventFactory
from iot_flink_pipeline.kafka.producer import IoTKafkaProducer
from iot_flink_pipeline.logging_config import setup_logging
from iot_flink_pipeline.settings import settings

logger = logging.getLogger(__name__)

DELAY_MEAN_SEC = 5.0
DELAY_STD_SEC = 2.0
MIN_DELAY_SEC = 0.5


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate IoT events into Kafka")
    parser.add_argument(
        "--events-per-batch",
        type=int,
        default=5,
        help="Number of events generated before each random delay",
    )
    return parser.parse_args()


def load_sensors_from_postgres() -> tuple[SensorSpec, ...]:
    query = """
        SELECT sensor_id, manufacturer_id
        FROM iot.sensors
        ORDER BY sensor_id;
    """

    with psycopg.connect(settings.postgres_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

    sensors = tuple(
        SensorSpec(
            sensor_id=str(sensor_id),
            manufacturer_id=int(manufacturer_id),
        )
        for sensor_id, manufacturer_id in rows
    )

    if not sensors:
        raise RuntimeError("No sensors found in Postgres table iot.sensors")

    return sensors


def generate_delay_sec() -> float:
    delay = random.normalvariate(DELAY_MEAN_SEC, DELAY_STD_SEC)
    return round(max(MIN_DELAY_SEC, delay), 2)


def produce_batch(
    *,
    factory: IoTEventFactory,
    producer: IoTKafkaProducer,
    events_per_batch: int,
) -> int:
    for _ in range(events_per_batch):
        event = factory.create_event()
        producer.produce(event)

    producer.poll()

    return events_per_batch


def main() -> None:
    setup_logging()

    args = parse_args()

    if args.events_per_batch <= 0:
        raise ValueError("--events-per-batch must be greater than 0")

    sensors = load_sensors_from_postgres()
    logger.info("Loaded %s sensors from Postgres", len(sensors))

    factory = IoTEventFactory(sensors=sensors)
    producer = IoTKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        topic=settings.iot_events_topic,
    )

    total_produced = 0

    logger.info(
        "Generator started: topic=%s, events_per_batch=%s, delay=N(%.1f, %.1f), min_delay=%.1f",
        settings.iot_events_topic,
        args.events_per_batch,
        DELAY_MEAN_SEC,
        DELAY_STD_SEC,
        MIN_DELAY_SEC,
    )

    try:
        while True:
            produced_in_batch = produce_batch(
                factory=factory,
                producer=producer,
                events_per_batch=args.events_per_batch,
            )
            total_produced += produced_in_batch

            delay_sec = generate_delay_sec()

            logger.info(
                "Produced batch: batch_size=%s, total_produced=%s, next_delay_sec=%s",
                produced_in_batch,
                total_produced,
                delay_sec,
            )

            time.sleep(delay_sec)

    except KeyboardInterrupt:
        logger.info("Generator stopped by user")

    finally:
        producer.flush()
        logger.info("Producer flushed. Total produced: %s", total_produced)


if __name__ == "__main__":
    main()