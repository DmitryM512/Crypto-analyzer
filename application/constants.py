__all__ = ["BINANCE_REQUIRE_COLUMNS", "BINANCE_DROP_COLUMNS"]

BINANCE_REQUIRE_COLUMNS = [
    "Date",
    "Open price",
    "High price",
    "Low price",
    "Close price",
    "Volume", # Base asset volume
    "Kline Close time",
    "Quote asset volume", # how much of the Quote asset is required to buy 1 unit of the Base asset
    "Number of trades",
    "Taker buy base asset volume", # Volume in BNB of Taker Buy(=Maker(market) Sell )
    "Taker buy quote asset volume", #  Volume in USDT of Taker Buy(=Maker(market) Buy
    "Unused field",
]
BINANCE_DROP_COLUMNS = [
    "Kline Close time",
    "Quote asset volume",
    "Number of trades",
    "Taker buy quote asset volume",
    "Unused field",
]
