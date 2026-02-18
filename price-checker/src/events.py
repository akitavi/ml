import time
from typing import Optional, Dict, Any


def build_price_event(
    ticker: str,
    ok: bool,
    price: Optional[float],
    error: Optional[str],
    source: str,
    elapsed_ms: int,
) -> Dict[str, Any]:
    return {
        "ts": int(time.time()),
        "ticker": ticker.upper(),
        "ok": ok,
        "price": price,
        "error": error,
        "source": source,
        "elapsed_ms": elapsed_ms,
    }

