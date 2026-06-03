import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone


@dataclass(frozen=True, slots=True)
class IoTEvent:
    sensor_id: str
    manufacturer_id: int
    event_time: datetime
    temperature: float
    humidity: float

    @property
    def kafka_key(self) -> bytes:
        return self.sensor_id.encode("utf-8")

    def to_kafka_value(self) -> bytes:
        payload = asdict(self)
        payload["event_time"] = self.event_time.astimezone(timezone.utc).isoformat()

        return json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")