import datetime
import os

from bitmex_historical_etl.constants import (
    BIGQUERY_INTERMEDIATE_TABLE_NAME,
    BIGQUERY_TABLE_NAME,
    CALCULATE_COLUMNS,
    COMBINE_TRADES,
    DOWNLOAD,
    UPDATE_SEQUENCE,
)

from .bigquery_loader import COMBINED_TRADE_SCHEMA, HISTORICAL_SCHEMA, BigQueryLoader
from .firestore_cache import FirestoreCache
from .s3_downloader import S3Downloader
from .transforms import (
    calculate_exponent,
    calculate_notional,
    combine_trades,
    prepare_s3,
    update_sequence,
)


class BitmexHistoricalETL:
    def __init__(
        self, date, steps, strip_nanoseconds=False, delete_intermediate_table=False
    ):
        self.date_string = date

        try:
            self.date = datetime.datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError as e:
            raise e
        else:
            min_date = datetime.date(2016, 5, 13)
            if self.date < min_date:
                date_string = min_date.isoformat()
                raise ValueError(
                    f"Minimum date: is {date_string}. "
                    "XBTUSD contract size was different before this date."
                )

        self.steps = steps
        self.strip_nanoseconds = strip_nanoseconds
        self.delete_intermediate_table = delete_intermediate_table

        self.firestore_cache = FirestoreCache(self.date)
        if not self.firestore_cache.stop_execution:
            self.bigquery_loader = BigQueryLoader(self.date)

    def main(self):
        if not self.firestore_cache.stop_execution:
            data_frame = None
            for step in self.steps:
                data = self.firestore_cache.get()
                if not data and step == DOWNLOAD:
                    data_frame = getattr(self, step)()
                elif data["step"] == step:
                    data_frame = getattr(self, step)(data_frame)

    def download(self):
        data_frame = S3Downloader().download(self.date)
        if data_frame is not None:
            data_frame = prepare_s3(
                self.date, data_frame, strip_nanoseconds=self.strip_nanoseconds
            )
            self.bigquery_loader.load(
                os.environ[BIGQUERY_INTERMEDIATE_TABLE_NAME],
                HISTORICAL_SCHEMA,
                data_frame,
            )
            symbols = data_frame["symbol"].unique().tolist()
            data = {
                "symbols": {symbol: {} for symbol in symbols},
                "step": UPDATE_SEQUENCE,
            }
            self.firestore_cache.set(data)
            print(f"Bitmex data: {self.date_string} downloaded")
        return data_frame

    def update_sequence(self, data_frame):
        if data_frame is None:
            data_frame = self.bigquery_loader.sequence_query()
        data_frame = update_sequence(data_frame)
        try:
            self.bigquery_loader.load(
                os.environ[BIGQUERY_INTERMEDIATE_TABLE_NAME],
                HISTORICAL_SCHEMA,
                data_frame,
            )
        except Exception as e:
            self.firestore_cache.delete()
            raise e
        else:
            data = self.firestore_cache.get()
            data["step"] = COMBINE_TRADES
            data = self.firestore_cache.set(data)
            print(f"Bitmex data: {self.date_string} sequenced")
            return data_frame

    def combine_trades(self, data_frame):
        if data_frame is None:
            data_frame = self.bigquery_loader.combine_trade_query()
        data_frame = combine_trades(data_frame)
        try:
            self.bigquery_loader.load(
                os.environ[BIGQUERY_TABLE_NAME], COMBINED_TRADE_SCHEMA, data_frame
            )
        except Exception as e:
            self.on_transform_exception()
            raise e
        else:
            if self.delete_intermediate_table:
                self.bigquery_loader.delete_table(
                    os.environ[BIGQUERY_INTERMEDIATE_TABLE_NAME]
                )
            data = self.firestore_cache.get()
            data["step"] = CALCULATE_COLUMNS
            data = self.firestore_cache.set(data)
            print(f"Bitmex data: {self.date_string} combined")
            return data_frame

    def calculate_columns(self, data_frame):
        if data_frame is None:
            data_frame = self.bigquery_loader.calculation_query()
        data_frame = calculate_notional(data_frame)
        print(f"Bitmex data: {self.date_string} notional calculated")
        data_frame = calculate_exponent(data_frame)
        print(f"Bitmex data: {self.date_string} exponent calculated")
        try:
            self.bigquery_loader.load(
                os.environ[BIGQUERY_TABLE_NAME], COMBINED_TRADE_SCHEMA, data_frame
            )
        except Exception as e:
            self.on_transform_exception()
            raise e
        else:
            data = self.firestore_cache.get()
            del data["step"]
            data["ok"] = True
            data = self.firestore_cache.set(data)
            print(f"Bitmex data: {self.date_string} OK")
            return data_frame

    def on_transform_exception(self):
        data = self.firestore_cache.get()
        data["step"] = COMBINE_TRADES
        self.firestore_cache.set(data)
