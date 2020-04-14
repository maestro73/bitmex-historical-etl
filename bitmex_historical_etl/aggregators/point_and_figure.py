from collections import OrderedDict

import pandas as pd
from google.cloud import bigquery

from ..bigquery_loader import get_schema_columns
from .base import BaseAggregator

POINT_AND_FIGURE_SCHEMA = [
    bigquery.SchemaField("symbol", "STRING", "REQUIRED"),
    bigquery.SchemaField("date", "DATE", "REQUIRED"),
    bigquery.SchemaField("duration", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("level", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("close", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("buyVolume", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("sellVolume", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("buyNotional", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("sellNotional", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("buyTicks", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("sellTicks", "INTEGER", "REQUIRED"),
]


class PointFigureAggregator(BaseAggregator):
    def __init__(self, date, thresh_attr, thresh_value, reversal=1):
        super().__init__(date, thresh_attr, thresh_value)

        self.reversal = reversal
        self.columns = get_schema_columns(POINT_AND_FIGURE_SCHEMA)
        self.keys = ("level",)

    def init_cache(self, data_frame):
        row = data_frame.iloc[0]
        cache = {
            "level": self.get_level(row),
        }
        return cache

    def is_cache_concat(self, cache):
        keys = [c for c in self.columns if c not in ("date",)]
        return all([key in cache for key in keys])

    def get_level(self, row):
        return int(row.price / self.thresh_value) * self.thresh_value

    def get_bounds(self, level, direction=None):
        if direction == 1:
            high = level + self.thresh_value
            low = level - (self.thresh_value * self.reversal)
        elif direction == -1:
            high = level + (self.thresh_value * self.reversal)
            low = level - self.thresh_value
        else:
            high = level + self.thresh_value
            low = level - self.thresh_value
        return high, low

    def apply_data_frame(self, data_frame):
        data_frame["level"] = None
        return super().apply_data_frame(data_frame)

    def aggregate(self, data_frame, cache):
        start = 0
        samples = []
        for index, row in data_frame.iterrows():
            high, low = self.get_bounds(cache["level"], cache["direction"])
            higher = row.price >= high
            lower = row.price <= low
            if higher or lower:
                change = abs(cache["level"] - self.get_level(row))
                if higher:
                    cache["direction"] = 1
                    cache["level"] += change
                else:
                    # Did price break below threshold?
                    if row.price != low:
                        # Is price exactly at a new level?
                        if row.price % self.thresh_value != 0:
                            # Only partial, decrement change.
                            change -= self.thresh_value
                    cache["direction"] = -1
                    cache["level"] -= change
                sample = self.aggregate_rows(data_frame, row, cache, start, stop=index)
                samples.append(sample)
                cache["timestamp"] = sample["timestamp"]
                # Always bounded.
                high, low = self.get_bounds(cache["level"])
                assert high >= row.price >= low
                assert high == low + (self.thresh_value * 2)
                start = index + 1
        cache = self.update_cache(data_frame, row, cache, start)
        df = pd.DataFrame(samples, columns=self.columns)
        return df, cache

    def aggregate_rows(self, data_frame, row, cache, start, stop=None):
        data = super().aggregate_rows(data_frame, row, cache, start, stop=stop)
        data["close"] = row["close"]
        data["level"] = cache["level"]
        if stop is not None:
            return OrderedDict([(key, data[key]) for key in self.columns])
        else:
            return data
