iot-flink-pipeline/
├── pyproject.toml
├── README.md
├── docker-compose.yml
├── .env.example
│
├── jars/
│   ├── flink-sql-connector-kafka-4.0.1-2.0.jar
│   ├── flink-connector-jdbc-core-4.0.0-2.0.jar
│   ├── flink-connector-jdbc-postgres-4.0.0-2.0.jar
│   └── postgresql-42.7.11.jar
│
├── sql/
│   ├── postgres/
│   │   ├── 001_create_device_types.sql
│   │   └── 002_insert_device_types.sql
│   │
│   └── flink/
│       ├── 001_create_kafka_source.sql
│       ├── 002_create_pg_source.sql
│       ├── 003_create_kafka_sink.sql
│       └── 004_insert_aggregates.sql
│
├── scripts/
│   ├── init_db.sh
│   ├── create_topics.sh
│   └── consume_results.sh
│
├── src/
│   └── iot_flink_pipeline/
│       ├── __init__.py
│       │
│       ├── domain/
│       │   ├── __init__.py
│       │   └── events.py
│       │
│       ├── generator/
│       │   ├── __init__.py
│       │   ├── event_factory.py
│       │   └── kafka_producer.py
│       │
│       ├── flink_jobs/
│       │   ├── __init__.py
│       │   ├── table_job.py
│       │   ├── datastream_bridge_job.py
│       │   └── common.py
│       │
│       └── entrypoints/
│           ├── __init__.py
│           ├── run_generator.py
│           ├── run_flink_table_job.py
│           └── run_result_consumer.py
│
└── tests/
    └── ...