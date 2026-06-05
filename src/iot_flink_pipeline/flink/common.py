from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

from iot_flink_pipeline.settings import settings


def create_stream_environment() -> StreamExecutionEnvironment:
    """Create Flink DataStream environment with project JARs."""
    config = Configuration()
    config.set_string("pipeline.jars", settings.pipeline_jars)
    config.set_string(
        "execution.checkpointing.interval",
        settings.flink_checkpoint_interval,
    )

    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(settings.flink_parallelism)

    return env


def create_stream_and_table_environment() -> tuple[
    StreamExecutionEnvironment,
    StreamTableEnvironment,
]:
    """Create shared DataStream and Table API environments."""
    env = create_stream_environment()

    table_settings = EnvironmentSettings.in_streaming_mode()
    t_env = StreamTableEnvironment.create(
        stream_execution_environment=env,
        environment_settings=table_settings,
    )

    # Important for Table API event-time windows over Kafka.
    # Without this, idle Kafka partitions can block watermark progress forever.
    t_env.get_config().set("table.exec.source.idle-timeout", "10s")

    return env, t_env


def create_table_environment() -> StreamTableEnvironment:
    """Create Table API environment."""
    _, t_env = create_stream_and_table_environment()
    return t_env