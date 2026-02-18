import requests


class YahooQuoteSource:
    """
    Берём цену напрямую из Yahoo quote endpoint (минуя yfinance),
    обычно обновляется заметно "живее" и без yfinance-кэшей.
    """

    BASE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"

    def __init__(self, timeout: float = 3.5):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0 Safari/537.36",
                "Accept": "application/json",
            }
        )

    def get_price(self, ticker: str) -> float:
        symbol = ticker.strip().upper()
        r = self.session.get(
            self.BASE_URL,
            params={"symbols": symbol},
            timeout=self.timeout,
        )
        r.raise_for_status()
        data = r.json()

        result = (data.get("quoteResponse") or {}).get("result") or []
        if not result:
            raise RuntimeError(f"Yahoo quote: empty result for {symbol}")

        q = result[0]
        price = q.get("regularMarketPrice")
        if price is None:
            # иногда бывает только pre/post/marketState — но для крипты обычно regularMarketPrice есть
            raise RuntimeError(f"Yahoo quote: no regularMarketPrice for {symbol}")

        try:
            price_f = float(price)
        except Exception:
            raise RuntimeError(f"Yahoo quote: invalid price {price!r} for {symbol}")

        if price_f <= 0:
            raise RuntimeError(f"Yahoo quote: non-positive price {price_f} for {symbol}")

        return price_f

    def get_quote_meta(self, ticker: str) -> dict:
        """
        Опционально: если хочешь отладку — вернём marketTime и т.п.
        """
        symbol = ticker.strip().upper()
        r = self.session.get(self.BASE_URL, params={"symbols": symbol}, timeout=self.timeout)
        r.raise_for_status()
        data = r.json()
        result = (data.get("quoteResponse") or {}).get("result") or []
        if not result:
            return {}
        q = result[0]
        return {
            "regularMarketTime": q.get("regularMarketTime"),
            "marketState": q.get("marketState"),
            "currency": q.get("currency"),
        }

