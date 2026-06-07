import json
from datetime import datetime, timezone
from typing import Any, Iterable, cast

from pyflink.common import Duration, Time, Types
from pyflink.common.watermark_strategy import (
    TimestampAssigner,
    WatermarkStrategy,
)
from pyflink.datastream import DataStream
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.table import StreamTableEnvironment, Table

from iot_flink_pipeline.settings import settings

def median(values: list[float]) -> float:
    """Calculate exact median for a non-empty list of float values."""
    sorted_values = sorted(values)
    n = len(sorted_values)
    mid = n // 2

    if n % 2 == 1:
        return sorted_values[mid]

    return (sorted_values[mid - 1] + sorted_values[mid]) / 2

def parse_event_time_to_epoch_millis(value: str) -> int:
    """Convert ISO event_time string to UTC epoch milliseconds."""
    parsed = datetime.fromisoformat(value)

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)

    return int(parsed.timestamp() * 1000)


def window_bound_to_string(value_ms: int) -> str:
    """Format window boundary milliseconds as a readable UTC timestamp."""
    return datetime.fromtimestamp(
        value_ms / 1000,
        tz=timezone.utc,
    ).strftime("%Y-%m-%d %H:%M:%S.000")


def get_window_start_ms(context: ProcessWindowFunction.Context) -> int:
    """Extract window start timestamp in milliseconds from PyFlink context."""
    window = context.window()
    start = cast(Any, window).start

    if callable(start):
        return int(cast(Any, start)())

    return int(cast(Any, start))


def get_window_end_ms(context: ProcessWindowFunction.Context) -> int:
    """Extract window end timestamp in milliseconds from PyFlink context."""
    window = context.window()
    end = cast(Any, window).end

    if callable(end):
        return int(cast(Any, end)())

    return int(cast(Any, end))


class TupleEventTimeAssigner(TimestampAssigner):
    """Use tuple field event_ts_ms as event-time timestamp."""

    def extract_timestamp(
        self,
        value: tuple[int, str, str, int, float, float],
        record_timestamp: int,
    ) -> int:
        return int(value[3])


class WindowResultJsonFunction(ProcessWindowFunction):
    """Aggregate one manufacturer window and emit one JSON result."""

    def process(
        self,
        key: int,
        context: ProcessWindowFunction.Context,
        elements: Iterable[tuple[int, str, str, int, float, float]],
    ) -> Iterable[str]:
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

        payload = {
            "window_start": window_bound_to_string(
                get_window_start_ms(context)
            ),
            "window_end": window_bound_to_string(
                get_window_end_ms(context)
            ),
            "manufacturer_id": int(first[0]),
            "manufacturer_name": first[1],
            "country": first[2],
            "events_count": events_count,
            "avg_temperature": avg_temperature,
            "median_humidity": median_humidity,
        }

        yield json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
        )


def create_simple_enriched_tuple_stream(enriched_stream: DataStream) -> DataStream:
    """Convert enriched Row stream to simple tuple stream with primitive types."""
    return enriched_stream.map(
        lambda row: (
            int(cast(Any, row).manufacturer_id),
            str(cast(Any, row).manufacturer_name),
            str(cast(Any, row).country),
            parse_event_time_to_epoch_millis(str(cast(Any, row).event_time)),
            float(cast(Any, row).temperature),
            float(cast(Any, row).humidity),
        ),
        output_type=Types.TUPLE(
            [
                Types.SHORT(),
                Types.STRING(),
                Types.STRING(),
                Types.LONG(),
                Types.DOUBLE(),
                Types.DOUBLE(),
            ]
        ),
    )


def assign_event_time_watermarks(simple_stream: DataStream) -> DataStream:
    """Assign event-time timestamps and bounded-out-of-orderness watermarks."""
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(settings.watermark_delay))
        .with_timestamp_assigner(TupleEventTimeAssigner())
    )

    return simple_stream.assign_timestamps_and_watermarks(watermark_strategy)


def create_window_result_json_stream_from_enriched_stream(
    enriched_stream: DataStream,
) -> DataStream:
    """Create final JSON window results from enriched DataStream rows."""
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
            WindowResultJsonFunction(),
            output_type=Types.STRING(),
        )
    )


def create_window_result_json_stream_from_table(
    *,
    t_env: StreamTableEnvironment,
    enriched_table: Table,
) -> DataStream:
    """Convert enriched Table to DataStream and build event-time window results."""
    enriched_stream = t_env.to_data_stream(enriched_table)

    return create_window_result_json_stream_from_enriched_stream(
        enriched_stream
    )