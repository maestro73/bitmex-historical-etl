import datetime

import pandas as pd

from .bigquery_loader import BigQueryLoader
from .firestore_cache import FirestoreCache
from .s3_downloader import S3Downloader


class BitmexHistoricalETL:
    def __init__(self, date):
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
            "foreignNotional",
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
        # Bitmex data is accurate to the millisecond.
        # However, data is only provided to 6 decimal places, not 9.
        # In case it is provided in the future, check for unconverted data.
        try:
            for i in range(100):
                datetime.datetime.strptime(
                    data_frame.iloc[i].timestamp, timestamp_format
                )
        except ValueError as error:
            _, value = str(error).split(": ")
            if int(value):
                raise error

        data_frame["timestamp"] = pd.to_datetime(
            data_frame["timestamp"], format=timestamp_format
        )
        data_frame = data_frame.astype(
            {
                "timestamp": "datetime64",
                "price": "float64",
                "size": "int64",
                "foreignNotional": "float64",
            }
        )
        data_frame.insert(0, "date", data_frame["timestamp"].dt.date)
        data_frame["volume"] = data_frame["size"]
        data_frame["tickRule"] = data_frame.apply(
            lambda x: (1 if x.tickDirection in ("PlusTick", "ZeroPlusTick") else -1),
            axis=1,
        )
        symbols = data_frame["symbol"].unique()
        all_df = []
        for symbol in symbols:
            df = data_frame[data_frame["symbol"] == symbol]
            df = df.copy()
            df.reset_index(drop=True, inplace=True)
            df["index"] = df.index
            all_df.append(df)
        data_frame = pd.concat(all_df)
        return data_frame[self.columns]
