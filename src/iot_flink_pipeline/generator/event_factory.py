import random
from datetime import datetime, timedelta, timezone

from iot_flink_pipeline.domain.events import IoTEvent
from iot_flink_pipeline.domain.sensors import SensorSpec


EVENT_TIME_JITTER_STD_SEC = 3.0
EVENT_TIME_JITTER_LIMIT_SEC = 4.0


class IoTEventFactory:
    def __init__(self, sensors: tuple[SensorSpec, ...]) -> None:
        self._sensors = sensors

    def create_event(self) -> IoTEvent:
        sensor = random.choice(self._sensors)

        return IoTEvent(
            sensor_id=sensor.sensor_id,
            manufacturer_id=sensor.manufacturer_id,
            event_time=self._generate_event_time(),
            temperature=self._generate_temperature(),
            humidity=self._generate_humidity(),
        )

    @staticmethod
    def _generate_event_time() -> datetime:
        jitter_sec = random.normalvariate(0.0, EVENT_TIME_JITTER_STD_SEC)
        bounded_jitter_sec = max(
            -EVENT_TIME_JITTER_LIMIT_SEC,
            min(EVENT_TIME_JITTER_LIMIT_SEC, jitter_sec),
        )

        return datetime.now(timezone.utc) + timedelta(seconds=bounded_jitter_sec)

    @staticmethod
    def _generate_temperature() -> float:
        return round(random.normalvariate(22.0, 5.0), 2)

    @staticmethod
    def _generate_humidity() -> float:
        value = random.normalvariate(55.0, 15.0)
        bounded_value = max(0.0, min(100.0, value))
        return round(bounded_value, 2)