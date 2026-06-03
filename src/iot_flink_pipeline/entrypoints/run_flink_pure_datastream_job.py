from iot_flink_pipeline.logging_config import setup_logging
from iot_flink_pipeline.flink.jobs.pure_datastream_job import run


def main() -> None:
    setup_logging()
    run()


if __name__ == "__main__":
    main()