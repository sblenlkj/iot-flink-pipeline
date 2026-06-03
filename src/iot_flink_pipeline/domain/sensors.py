from dataclasses import dataclass
from typing import Final


@dataclass(frozen=True, slots=True)
class SensorSpec:
    sensor_id: str
    manufacturer_id: int
