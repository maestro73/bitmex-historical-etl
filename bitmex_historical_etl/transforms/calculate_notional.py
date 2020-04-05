from ..constants import ETHUSD, XBTUSD, XRPUSD, uBTC


def calc_notional(x):
    if x["symbol"] == XBTUSD:
        return x["volume"] / x.price
    elif x["symbol"] == ETHUSD:
        return x["volume"] * x["price"] * uBTC
    elif x["symbol"] == XRPUSD:
        return x["volume"] * x["price"] * uBTC / 20
    elif "USD" in x["symbol"]:
        raise NotImplementedError
    else:
        return x["volume"] * x["price"]


def calculate_notional(data_frame):
    data_frame["notional"] = data_frame.apply(calc_notional, axis=1)
    return data_frame
