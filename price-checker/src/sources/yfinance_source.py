import yfinance as yf


class YFinanceSource:
    """
    Источник цены через yfinance.
    Оставляем отдельным модулем, чтобы заменить на другой источник одной строкой.
    """

    def get_price(self, ticker: str) -> float:
        t = yf.Ticker(ticker)

        # Быстрый путь (если доступно)
        fi = getattr(t, "fast_info", None)
        if fi and isinstance(fi, dict):
            for key in ("last_price", "lastPrice", "regularMarketPrice", "last_close", "lastClose"):
                v = fi.get(key)
                if isinstance(v, (int, float)) and v > 0:
                    return float(v)

        # Надежный фоллбек
        hist = t.history(period="1d", interval="1m")
        if hist is None or hist.empty:
            hist = t.history(period="5d", interval="1d")
        if hist is None or hist.empty:
            raise RuntimeError("No price data returned by yfinance")

        close_series = hist.get("Close")
        if close_series is None or close_series.empty:
            raise RuntimeError("No Close column in yfinance history")

        price = float(close_series.iloc[-1])
        if price <= 0:
            raise RuntimeError(f"Invalid price: {price}")
        return price

