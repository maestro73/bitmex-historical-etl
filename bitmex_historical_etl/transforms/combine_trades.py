import pandas as pd


def calc_average_price(x):
    average_price = sum((x["price"] * x["volume"]) / x["volume"].sum())
    if "USD" in x["symbol"]:
        average_price = round(average_price, 2)
    return average_price


def combine_trades(data_frame):
    group = data_frame.groupby(["symbol", "timestamp", "sequence"])
    symbol = group.symbol.last()
    price = group.price.last()
    # Must calculate average price this transform.
    average_price = group.apply(calc_average_price)
    volume = group.volume.sum()
    tick_rule = group.tickRule.last()
    columns = {
        "symbol": symbol,
        "price": price,
        "averagePrice": average_price,
        "volume": volume,
        "tickRule": tick_rule,
        "exponent": 0,
        "notional": 0,
    }
    df = pd.DataFrame(columns)
    df.index = df.index.droplevel(("symbol", "sequence"))
    df = df.reset_index()
    df.insert(0, "date", df["timestamp"].dt.date)
    return df[["timestamp", "date"] + [c for c in columns]]
