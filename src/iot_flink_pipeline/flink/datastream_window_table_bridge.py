from typing import Iterable

from pyflink.common import Row, Time, Types
from pyflink.datastream import DataStream
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.table import DataTypes, Schema, StreamTableEnvironment, Table

from iot_flink_pipeline.flink.datastream_windows import (
    assign_event_time_watermarks,
    create_simple_enriched_tuple_stream,
    get_window_end_ms,
    get_window_start_ms,
    window_bound_to_string,
    median,
)


class WindowResultRowFunction(ProcessWindowFunction):
    """Aggregate one manufacturer window and emit one Row for Table API sink."""

    def process(
        self,
        key: int,
        context: ProcessWindowFunction.Context,
        elements: Iterable[tuple[int, str, str, int, float, float]],
    ) -> Iterable[Row]:
        rows = list(elements)

        if not rows:
            return []

        first = rows[0]
        events_count = len(rows)

        avg_temperature = round(
            sum(row[4] for row in rows) / events_count,
            2,
        )
        median_humidity = round(
            median([row[5] for row in rows]),
            2,
        )

        yield Row(
            window_start=window_bound_to_string(get_window_start_ms(context)),
            window_end=window_bound_to_string(get_window_end_ms(context)),
            manufacturer_id=int(first[0]),
            manufacturer_name=first[1],
            country=first[2],
            events_count=events_count,
            avg_temperature=avg_temperature,
            median_humidity=median_humidity,
        )


def create_window_result_row_stream_from_enriched_stream(
    enriched_stream: DataStream,
) -> DataStream:
    """Create Row window results from enriched DataStream rows."""
    simple_stream = create_simple_enriched_tuple_stream(enriched_stream)
    event_time_stream = assign_event_time_watermarks(simple_stream)

    return (
        event_time_stream
        .key_by(
            lambda row: row[0],
            key_type=Types.SHORT(),
        )
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .process(
            WindowResultRowFunction(),
            output_type=Types.ROW_NAMED(
                [
                    "window_start",
                    "window_end",
                    "manufacturer_id",
                    "manufacturer_name",
                    "country",
                    "events_count",
                    "avg_temperature",
                    "median_humidity",
                ],
                [
                    Types.STRING(),
                    Types.STRING(),
                    Types.SHORT(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.LONG(),
                    Types.DOUBLE(),
                    Types.DOUBLE(),
                ],
            ),
        )
    )


def create_window_result_row_stream_from_table(
    *,
    t_env: StreamTableEnvironment,
    enriched_table: Table,
) -> DataStream:
    """Convert enriched Table to DataStream and build Row window results."""
    enriched_stream = t_env.to_data_stream(enriched_table)

    return create_window_result_row_stream_from_enriched_stream(
        enriched_stream
    )


def create_window_result_table(
    *,
    t_env: StreamTableEnvironment,
    result_stream: DataStream,
) -> Table:
    """Convert Row window result DataStream back to Table API table."""
    return t_env.from_data_stream(
        result_stream,
        Schema.new_builder()
        .column("window_start", DataTypes.STRING())
        .column("window_end", DataTypes.STRING())
        .column("manufacturer_id", DataTypes.SMALLINT())
        .column("manufacturer_name", DataTypes.STRING())
        .column("country", DataTypes.STRING())
        .column("events_count", DataTypes.BIGINT())
        .column("avg_temperature", DataTypes.DOUBLE())
        .column("median_humidity", DataTypes.DOUBLE())
        .build(),
    )