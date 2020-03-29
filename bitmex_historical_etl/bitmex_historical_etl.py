import datetime
from datetime import timezone

import numpy as np
import pandas as pd

from .bigquery_loader import BigQueryLoader
from .firestore_cache import FirestoreCache
from .s3_downloader import S3Downloader


class BitmexHistoricalETL:
    def __init__(self, date, strip_nanoseconds=False):
        self.strip_nanoseconds = strip_nanoseconds

        try:
            self.date = datetime.datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError as e:
            raise e
        else:
            # Data for XBTUSD exists before this date.
            # However, size was determined differently.
            assert self.date >= datetime.date(2016, 5, 13)

        self.columns = [
            "symbol",
            "date",
            "timestamp",
            "price",
            "volume",
            "tickRule",
            "index",
        ]

    def main(self):
        firestore_cache = FirestoreCache(self.date)
        if not firestore_cache.stop_execution:
            data_frame = S3Downloader().download(self.date)
            if data_frame is not None:
                df = self.validate_data_frame(data_frame)
                bigquery_loader = BigQueryLoader(self.date)
                bigquery_loader.load(df)
                symbols = df["symbol"].unique().tolist()
                firestore_cache.set_cache(symbols)
                date_string = self.date.isoformat()
                print(f"{date_string} OK")

    def validate_data_frame(self, data_frame):
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
            date_string = self.date.isoformat()
            rows = "row" if total == 1 else "rows"
            print(f"Unsupported nanoseconds: {total} {rows} on {date_string}")
            if self.strip_nanoseconds:
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
        return data_frame[self.columns]
