from sources.yfinance_source import YFinanceSource
from sources.yahoo_quote_source import YahooQuoteSource
from sources.binance_source import BinanceSpotSource

_SOURCES = {
    "yfinance": YFinanceSource,
    "yahoo": YahooQuoteSource,
    "binance": BinanceSpotSource,
}

def get_source(name: str):
    cls = _SOURCES.get(name)
    if not cls:
        raise ValueError(f"Unknown price source: {name}")
    return cls()
