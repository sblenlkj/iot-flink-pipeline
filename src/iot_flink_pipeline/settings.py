from pathlib import Path
from typing import ClassVar

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Hardcoded project topics.
    # These topic names must match docker-compose kafka-init section.
    iot_events_topic: ClassVar[str] = "iot_events"

    iot_aggregates_upsert_topic: ClassVar[str] = "iot_aggregates_upsert"
    iot_window_results_topic: ClassVar[str] = "iot_window_results"
    iot_window_results_table_sink_topic: ClassVar[str] = (
        "iot_window_results_table_sink"
    )
    iot_window_results_datastream_topic: ClassVar[str] = (
        "iot_window_results_datastream"
    )

    kafka_bootstrap_servers: str = "localhost:9092"

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "iot_flink"
    postgres_user: str = "iot_user"
    postgres_password: str = "iot_password"

    flink_parallelism: int = 1
    flink_checkpoint_interval: str = "10s"
    watermark_delay: int = 10

    kafka_connector_jar: str
    jdbc_core_jar: str
    jdbc_postgres_jar: str
    postgres_driver_jar: str

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def postgres_jdbc_url(self) -> str:
        return (
            f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/"
            f"{self.postgres_db}"
        )

    @property
    def pipeline_jars(self) -> str:
        jar_paths = [
            self.kafka_connector_jar,
            self.jdbc_core_jar,
            self.jdbc_postgres_jar,
            self.postgres_driver_jar,
        ]

        return ";".join(
            f"file://{Path(jar_path).expanduser().resolve()}"
            for jar_path in jar_paths
        )


settings = Settings()  # type: ignore