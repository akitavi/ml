from typing import Protocol


class PriceSource(Protocol):
    def get_price(self, ticker: str) -> float:
        ...
