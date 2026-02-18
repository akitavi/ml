from prometheus_client import Gauge, Counter


# 🔹 Текущая цена тикера
ticker_price = Gauge(
    "ticker_price",
    "Last polled ticker price",
    ["ticker", "source", "currency"],
)


# 🔹 1 если тикер включен, 0 если выключен
ticker_enabled = Gauge(
    "ticker_enabled",
    "Ticker enabled in watchlist (1=ON, 0=OFF)",
    ["ticker"],
)


# 🔹 Unix time последнего успешного обновления
ticker_last_update_ts = Gauge(
    "ticker_last_update_ts",
    "Last successful price update timestamp (unix seconds)",
    ["ticker"],
)


# 🔹 Ошибки получения цен
fetch_errors_total = Counter(
    "ticker_fetch_errors_total",
    "Total errors while fetching ticker price",
    ["ticker"],
)
