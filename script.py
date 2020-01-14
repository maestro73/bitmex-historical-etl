from datetime import datetime

import fire
import pandas as pd

from bitmex_historical_etl.utils import set_environment
from main import BitmexHistoricalETL


def get_bitmex_historical(date="2016-05-13"):
    set_environment()
    today = datetime.now().date().isoformat()
    timestamps = pd.date_range(start=date, end=today)
    dates = [d.date().isoformat() for d in timestamps.tolist()]
    for date in dates:
        controller = BitmexHistoricalETL(date)
        controller.main()


if __name__ == "__main__":
    fire.Fire(get_bitmex_historical)
