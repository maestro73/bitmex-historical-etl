from datetime import datetime

import pandas as pd

import typer
from bitmex_historical_etl.utils import set_environment
from main import BitmexHistoricalETL


def get_bitmex_historical(date: str = "2016-05-13", strip_nanoseconds: bool = False):
    set_environment()
    today = datetime.now().date().isoformat()
    timestamps = pd.date_range(start=date, end=today)
    dates = [d.date().isoformat() for d in timestamps.tolist()]
    for date in dates:
        controller = BitmexHistoricalETL(date, strip_nanoseconds=strip_nanoseconds)
        controller.main()


if __name__ == "__main__":
    typer.run(get_bitmex_historical)
