import datetime
from datetime import timezone

import pandas as pd


class BaseAggregator:
    def __init__(self, date, thresh_attr, thresh_value):

        self.timestamp = datetime.datetime.combine(date, datetime.datetime.min.time())

        self.thresh_attr = thresh_attr
        self.thresh_value = thresh_value

        # Implemented by subclasses.
        self.columns = None
        self.keys = None

    def is_cache_valid(self, cache):
        return all([cache.get(key, None) is not None for key in self.keys])

    def is_cache_concat(self, cache):
        keys = [c for c in self.columns if c not in ("date",)]
        return all([key in cache for key in keys])

    def concat_cache(self, data_frame, cache):
        # If valid, concat.
        if self.is_cache_concat(cache):
            df = pd.DataFrame([cache], columns=self.columns)
            data_frame = pd.concat([df, data_frame])
            data_frame.reset_index(drop=True, inplace=True)
        return data_frame

    def apply_data_frame(self, data_frame):
        data_frame["ticks"] = 1
        data_frame["buyVolume"] = data_frame.apply(
            lambda x: x.volume if x.tickRule == 1 else 0, axis=1
        )
        data_frame["sellVolume"] = data_frame.apply(
            lambda x: x.volume if x.tickRule == -1 else 0, axis=1
        )
        data_frame["buyNotional"] = data_frame.apply(
            lambda x: x.volume / x.price if x.tickRule == 1 else 0, axis=1
        )
        data_frame["sellNotional"] = data_frame.apply(
            lambda x: x.volume / x.price if x.tickRule == -1 else 0, axis=1
        )
        data_frame["buyTicks"] = data_frame.apply(
            lambda x: x.ticks if x.tickRule == 1 else 0, axis=1
        )
        data_frame["sellTicks"] = data_frame.apply(
            lambda x: x.ticks if x.tickRule == -1 else 0, axis=1
        )
        return data_frame[self.columns]

    def get_sample(self, data_frame, start, stop=None):
        s = start - 1
        if stop is not None:
            sample = data_frame.loc[s:stop]
        else:
            sample = data_frame.loc[s:]
        return sample

    def aggregate_rows(self, data_frame, row, cache, start, stop=None):
        # Timestamp
        timestamp = row.timestamp if stop else data_frame.loc[start].timestamp
        timestamp = timestamp.replace(tzinfo=timezone.utc)
        sample = self.get_sample(data_frame, start, stop=stop)
        s = sample.loc[1:] if start > 0 else sample
        data = {
            "timestamp": timestamp,
            "buyVolume": s["buyVolume"].sum(),
            "sellVolume": s["sellVolume"].sum(),
            "buyNotional": s["buyNotional"].sum(),
            "sellNotional": s["sellNotional"].sum(),
            "buyTicks": s["buyTicks"].sum(),
            "sellTicks": s["sellTicks"].sum(),
        }
        if stop is not None:
            data["date"] = row.date
            assert timestamp >= cache["timestamp"]
        return data

    def update_cache(self, data_frame, row, cache, start):
        # There was a remainder.
        if (start) < data_frame.shape[0]:
            data = self.aggregate_rows(data_frame, row, cache, start)
            cache.update(data)
        # There wasn't a remainder. Last row was sample.
        else:
            for key in list(cache.keys()):
                if key not in self.keys:
                    del cache[key]
        return cache
