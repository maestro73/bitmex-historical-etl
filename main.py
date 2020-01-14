import base64
import json

from bitmex_historical_etl import BitmexHistoricalETL
from bitmex_historical_etl.utils import get_delta


def get_bitmex_historical(event, context):
    yesterday = get_delta(days=-1)
    date = yesterday.isoformat()
    if "data" in event:
        data = base64.b64decode(event["data"]).decode()
        d = json.loads(data)
        # Download previous days data.
        if len(data):
            date = d.get("date", date)
    controller = BitmexHistoricalETL(date)
    controller.main()
