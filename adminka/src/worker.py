# worker.py
import os
from datetime import datetime, timezone

import requests
from sqlalchemy import select, or_, func
from sqlalchemy.orm import Session

from models import Watchlist, PriceTick
from metrics import ticker_price, ticker_last_update_ts, fetch_errors_total

PRICE_SERVICE_URL = os.getenv("PRICE_SERVICE_URL", "http://price_checker:8050")
PRICE_SOURCE = os.getenv("PRICE_SOURCE", "yfinance")
PRICE_HTTP_TIMEOUT = float(os.getenv("PRICE_HTTP_TIMEOUT", "3"))


def fetch_price(ticker: str) -> dict | None:
    url = f"{PRICE_SERVICE_URL}/price/{ticker}"
    params = {"source": PRICE_SOURCE}
    try:
        r = requests.get(url, params=params, timeout=PRICE_HTTP_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        fetch_errors_total.labels(ticker=ticker).inc()
        print(f"[ERROR] Fetch failed for {ticker}: {e}")
        return None


def poll_due_once(db: Session) -> int:
    """
    Одна итерация: находим тикеры, которым пора обновиться,
    дергаем внешний сервис, пишем в БД.
    Возвращает количество обработанных тикеров.
    """
    now = datetime.now(timezone.utc)

    # "пора" = last_polled_at is null OR прошло >= poll_seconds секунд
    due_stmt = (
        select(Watchlist)
        .where(Watchlist.is_on == True)
        .where(
            or_(
                Watchlist.last_polled_at.is_(None),
                (func.extract("epoch", func.now() - Watchlist.last_polled_at) >= Watchlist.poll_seconds),
            )
        )
        .order_by(Watchlist.ticker)
    )

    items = db.execute(due_stmt).scalars().all()
    if not items:
        return 0

    processed = 0
    for item in items:
        data = fetch_price(item.ticker)

        # чтобы не долбить тикер каждую секунду при ошибке — обновим last_polled_at в любом случае
        item.last_polled_at = now

        if not data or "price" not in data:
            continue

        price = float(data["price"])
        source = str(data.get("source") or PRICE_SOURCE)
        currency = str(data.get("currency") or "")

        db.add(
            PriceTick(
                ticker=item.ticker,
                price=price,
                ts=now,
                source=source,
                currency=currency,
            )
        )

        # метрики
        ticker_price.labels(ticker=item.ticker, source=source, currency=currency).set(price)
        ticker_last_update_ts.labels(ticker=item.ticker).set(int(now.timestamp()))

        processed += 1

    db.commit()
    return processed
