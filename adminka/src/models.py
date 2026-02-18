from __future__ import annotations

from datetime import datetime
from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Numeric,
    Text,
    func,
    Index,
    Integer
)
from sqlalchemy import Integer
from sqlalchemy.orm import Mapped, mapped_column
from db import Base

class Watchlist(Base):
    __tablename__ = "watchlist"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(Text, unique=True, nullable=False, index=True)
    is_on: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="true")

    poll_seconds: Mapped[int] = mapped_column(Integer, nullable=False, server_default="30")

    last_polled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)


class PriceTick(Base):
    __tablename__ = "price_ticks"

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True
    )

    ticker: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        index=True
    )

    ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        index=True
    )

    price: Mapped[float] = mapped_column(
        Numeric(18, 6),
        nullable=False
    )

    currency: Mapped[str | None] = mapped_column(
        Text,
        nullable=True
    )

    source: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default="yfinance"
    )


# 🔹 Композитный индекс для быстрого получения последней цены
Index(
    "idx_price_ticker_ts_desc",
    PriceTick.ticker,
    PriceTick.ts.desc()
)


class SchedulerState(Base):
    __tablename__ = "scheduler_state"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
