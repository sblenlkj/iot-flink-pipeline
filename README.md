

src/iot_flink_pipeline/flink_jobs/
├── __init__.py
├── common.py
├── ddl.py
├── joins.py
├── datastream_windows.py
├── datastream_sinks.py
├── table_join_to_datastream_kafka_job.py
├── table_only_upsert_job.py
└── table_join_datastream_window_table_sink_job.py

src/iot_flink_pipeline/entrypoints/
├── run_flink_datastream_window_job.py
├── run_flink_table_upsert_job.py
└── run_flink_bridge_table_sink_job.py