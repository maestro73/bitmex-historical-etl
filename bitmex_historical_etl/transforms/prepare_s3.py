from datetime import timezone

import numpy as np
import pandas as pd


def prepare_s3(date, data_frame, strip_nanoseconds=True):
    timestamp_format = "%Y-%m-%dD%H:%M:%S.%f"
    data_frame["timestamp"] = pd.to_datetime(
        data_frame["timestamp"], format=timestamp_format
    )
    # Because pyarrow.lib.ArrowInvalid: Casting from timestamp[ns]
    # to timestamp[us, tz=UTC] would lose data.
    data_frame["timestamp"] = data_frame.apply(
        lambda x: x.timestamp.tz_localize(timezone.utc), axis=1
    )
    # Bitmex data is accurate to the nanosecond.
    # However, data is typically only provided to the microsecond.
    data_frame["nanoseconds"] = data_frame.apply(
        lambda x: x.timestamp.nanosecond, axis=1
    )
    with_nanoseconds = data_frame[data_frame["nanoseconds"] > 0]
    # On 2017-09-08 there is one timestamp with nanoseconds.
    # If kwarg self.string_nanoeconds, then strip.
    total = len(with_nanoseconds)
    if total:
        date_string = date.isoformat()
        rows = "row" if total == 1 else "rows"
        print(f"Unsupported nanoseconds: {total} {rows} on {date_string}")
        if strip_nanoseconds:
            data_frame["timestamp"] = data_frame.apply(
                lambda x: x.timestamp.replace(nanosecond=0)
                if x.nanoseconds > 0
                else x.timestamp,
                axis=1,
            )
            data_frame["nanoseconds"] = data_frame.apply(
                lambda x: x.timestamp.nanosecond, axis=1
            )
            with_nanoseconds = data_frame[data_frame["nanoseconds"] > 0]
            assert len(with_nanoseconds) == 0
    data_frame.insert(0, "date", data_frame["timestamp"].dt.date)
    data_frame = data_frame.rename(columns={"size": "volume"})
    data_frame["tickRule"] = data_frame.apply(
        lambda x: (1 if x.tickDirection in ("PlusTick", "ZeroPlusTick") else -1),
        axis=1,
    )
    symbols = data_frame["symbol"].unique()
    data_frame["index"] = np.nan
    for symbol in symbols:
        index = data_frame.index[data_frame["symbol"] == symbol]
        # 0-based index according to symbol.
        data_frame.loc[index, "index"] = index.values - index.values[0]
    data_frame = data_frame.astype(
        {"price": "float64", "volume": "int64", "index": "int64"}
    )
    data_frame["sequence"] = 0
    return data_frame
