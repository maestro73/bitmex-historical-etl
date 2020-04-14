from collections import OrderedDict
from datetime import timezone

import pandas as pd
from google.cloud import bigquery

from ..bigquery_loader import get_schema_columns
from .base import BaseAggregator

TICK = "tick"
VOLUME = "volume"
NOTIONAL = "notional"


THRESH_SCHEMA = [
    bigquery.SchemaField("symbol", "STRING", "REQUIRED"),
    bigquery.SchemaField("date", "DATE", "REQUIRED"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", "REQUIRED"),
    bigquery.SchemaField("open", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("high", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("low", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("close", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("buyVolume", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("sellVolume", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("buyNotional", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("sellNotional", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("buyTicks", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("sellTicks", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("index", "INTEGER", "REQUIRED"),
]


class ThreshAggregator(BaseAggregator):
    def __init__(self, date, thresh_attr, thresh_value):
        super().__init__(date, thresh_attr, thresh_value)

        if thresh_attr == VOLUME:
            self.thresh_attrs = ["buyVolume", "sellVolume"]
        elif thresh_attr == NOTIONAL:
            self.thresh_attrs = ["buyNotional", "sellNotional"]
        elif thresh_attr == TICK:
            self.thresh_attrs = ["buyTicks", "sellTicks"]

        self.columns = get_schema_columns(THRESH_SCHEMA)
        self.keys = ["timestamp"] + self.thresh_attrs

    def init_cache(self, data_frame):
        cache = {"timestamp": self.timestamp.replace(tzinfo=timezone.utc)}
        for thresh_attr in self.thresh_attrs:
            cache[thresh_attr] = 0
        return cache

    def apply_data_frame(self, data_frame):
        for column in ("open", "high", "low", "close"):
            data_frame[column] = data_frame["price"]
        return super().apply_data_frame(data_frame)

    def aggregate(self, data_frame, cache):
        start = 0
        samples = []
        for index, row in data_frame.iterrows():
            # Do not add cache to itself.
            if index or not self.is_cache_concat(cache):
                for thresh_attr in self.thresh_attrs:
                    cache[thresh_attr] += row[thresh_attr]
            total = sum([abs(cache[thresh_attr]) for thresh_attr in self.thresh_attrs])
            should_sample = total >= self.thresh_value
            if should_sample:
                sample = self.aggregate_rows(
                    data_frame, row, cache, start, stop=index, price_attr="close"
                )
                samples.append(sample)
                cache["timestamp"] = sample["timestamp"]
                for thresh_attr in self.thresh_attrs:
                    cache[thresh_attr] = 0
                start = index + 1
        cache = self.update_cache(data_frame, row, cache, start, price_attr="close")
        df = pd.DataFrame(samples, columns=self.columns)
        return df, cache

    def aggregate_rows(self, data_frame, row, cache, start, stop=None, price_attr=None):
        data = super().aggregate_rows(
            data_frame, row, cache, start, stop=stop, price_attr=price_attr
        )
        sample = self.get_sample(data_frame, start, stop=stop)
        s = sample.loc[1:] if start > 0 else sample
        first_row = sample.iloc[0]
        # Is row from cache?
        if self.timestamp.date() == first_row.timestamp.date():
            # If no, close.
            open_price = first_row.close
        else:
            # Otherwise, open.
            open_price = first_row.open
        data["open"] = open_price
        data["low"] = s.low.min()
        data["high"] = s.high.max()
        data["close"] = row.close
        data["notional"] = s.notional.sum()
        if stop is not None:
            return OrderedDict([(key, data[key]) for key in self.columns])
        else:
            return data
