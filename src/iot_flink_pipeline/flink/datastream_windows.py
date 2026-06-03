import json
from datetime import datetime, timezone
from typing import Any, Iterable, cast

from pyflink.common import Row, Time, Types
from pyflink.datastream import DataStream
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.table import DataTypes, Schema, StreamTableEnvironment, Table


def _window_bound_to_string(value_ms: int) -> str:
    return datetime.fromtimestamp(
        value_ms / 1000,
        tz=timezone.utc,
    ).strftime("%Y-%m-%d %H:%M:%S.000")


def _get_window_start_ms(context: ProcessWindowFunction.Context) -> int:
    window = context.window()
    start = cast(Any, window).start

    if callable(start):
        return int(cast(Any, start)())

    return int(cast(Any, start))


def _get_window_end_ms(context: ProcessWindowFunction.Context) -> int:
    window = context.window()
    end = cast(Any, window).end

    if callable(end):
        return int(cast(Any, end)())

    return int(cast(Any, end))


def _calculate_window_values(
    *,
    context: ProcessWindowFunction.Context,
    elements: Iterable[Row],
) -> dict[str, object]:
    rows = list(elements)

    if not rows:
        raise ValueError("Window elements must not be empty")

    first = cast(Any, rows[0])
    rows_any = [cast(Any, row) for row in rows]

    events_count = len(rows_any)

    avg_temperature = round(
        sum(float(row.temperature) for row in rows_any) / events_count,
        2,
    )
    avg_humidity = round(
        sum(float(row.humidity) for row in rows_any) / events_count,
        2,
    )

    return {
        "window_start": _window_bound_to_string(_get_window_start_ms(context)),
        "window_end": _window_bound_to_string(_get_window_end_ms(context)),
        "manufacturer_id": int(first.manufacturer_id),
        "manufacturer_name": str(first.manufacturer_name),
        "country": str(first.country),
        "events_count": events_count,
        "avg_temperature": avg_temperature,
        "avg_humidity": avg_humidity,
    }


class WindowResultJsonFunction(ProcessWindowFunction):
    def process(
        self,
        key: int,
        context: ProcessWindowFunction.Context,
        elements: Iterable[Row],
    ) -> Iterable[str]:
        payload = _calculate_window_values(
            context=context,
            elements=elements,
        )

        yield json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
        )


class WindowResultRowFunction(ProcessWindowFunction):
    def process(
        self,
        key: int,
        context: ProcessWindowFunction.Context,
        elements: Iterable[Row],
    ) -> Iterable[Row]:
        payload = _calculate_window_values(
            context=context,
            elements=elements,
        )

        yield Row(
            window_start=payload["window_start"],
            window_end=payload["window_end"],
            manufacturer_id=payload["manufacturer_id"],
            manufacturer_name=payload["manufacturer_name"],
            country=payload["country"],
            events_count=payload["events_count"],
            avg_temperature=payload["avg_temperature"],
            avg_humidity=payload["avg_humidity"],
        )


def create_window_result_json_stream_from_enriched_stream(
    enriched_stream: DataStream,
) -> DataStream:
    return (
        enriched_stream
        .key_by(
            lambda row: cast(Any, row).manufacturer_id,
            key_type=Types.SHORT(),
        )
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .process(
            WindowResultJsonFunction(),
            output_type=Types.STRING(),
        )
    )


def create_window_result_row_stream_from_enriched_stream(
    enriched_stream: DataStream,
) -> DataStream:
    return (
        enriched_stream
        .key_by(
            lambda row: cast(Any, row).manufacturer_id,
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
                    "avg_humidity",
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


def create_window_result_json_stream_from_table(
    *,
    t_env: StreamTableEnvironment,
    enriched_table: Table,
) -> DataStream:
    enriched_stream = t_env.to_data_stream(enriched_table)

    return create_window_result_json_stream_from_enriched_stream(
        enriched_stream
    )


def create_window_result_row_stream_from_table(
    *,
    t_env: StreamTableEnvironment,
    enriched_table: Table,
) -> DataStream:
    enriched_stream = t_env.to_data_stream(enriched_table)

    return create_window_result_row_stream_from_enriched_stream(
        enriched_stream
    )


def create_window_result_table(
    *,
    t_env: StreamTableEnvironment,
    result_stream: DataStream,
) -> Table:
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
        .column("avg_humidity", DataTypes.DOUBLE())
        .build(),
    )