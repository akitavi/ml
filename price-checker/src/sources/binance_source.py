import requests


class BinanceSpotSource:
    """
    Берём цену через Binance Spot API:
    GET https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT
    """

    BASE_URL = "https://api.binance.com/api/v3/ticker/price"

    def __init__(self, timeout: float = 3.5):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def _to_symbol(self, ticker: str) -> str:
        t = ticker.strip().upper()

        # Удобные шорткаты:
        # BTC-USD / BTCUSD -> BTCUSDT (самый ликвидный паблик-референс)
        if t in ("BTC", "BTC-USD", "BTCUSD"):
            return "BTCUSDT"
        if t in ("ETH", "ETH-USD", "ETHUSD"):
            return "ETHUSDT"

        # Если пользователь уже передал BTCUSDT — оставляем
        if t.endswith("USDT"):
            return t

        # Можно расширить: BTC-EUR -> BTCEUR (если нужно)
        return t

    def get_price(self, ticker: str) -> float:
        symbol = self._to_symbol(ticker)
        r = self.session.get(self.BASE_URL, params={"symbol": symbol}, timeout=self.timeout)
        r.raise_for_status()
        data = r.json()
        price = float(data["price"])
        if price <= 0:
            raise RuntimeError(f"Invalid Binance price: {price} for {symbol}")
        return price
