# app.py
import os
import logging
from datetime import timedelta

from flask import Flask, render_template, request, redirect, url_for, Response
from apscheduler.schedulers.background import BackgroundScheduler
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from sqlalchemy import select, true, update, delete, func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from db import engine, SessionLocal
from models import Base, Watchlist, PriceTick
from worker import poll_due_once

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.jinja_env.globals.update(timedelta=timedelta)

# как часто шедулер “просыпается” и проверяет due тикеры
SCHED_TICK_SECONDS = int(os.getenv("SCHED_TICK_SECONDS", "1"))

_scheduler: BackgroundScheduler | None = None

logging.getLogger("apscheduler").setLevel(logging.WARNING)
logging.getLogger("apscheduler.executors.default").setLevel(logging.WARNING)
logging.getLogger("apscheduler.scheduler").setLevel(logging.WARNING)


def init_db() -> None:
    Base.metadata.create_all(bind=engine)


def start_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        return

    sched = BackgroundScheduler(daemon=True)

    def _job():
        try:
            with SessionLocal() as db:
                n = poll_due_once(db)
                if n:
                    logger.info("Polled %s ticker(s)", n)
        except Exception:
            logger.exception("Scheduler job failed")

    sched.add_job(
        _job,
        "interval",
        seconds=SCHED_TICK_SECONDS,
        id="poll_due",
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )
    sched.start()
    _scheduler = sched
    logger.info("Scheduler started: tick every %ss", SCHED_TICK_SECONDS)


def bootstrap() -> None:
    init_db()
    start_scheduler()


bootstrap()


@app.get("/")
def index():
    with SessionLocal() as db:  # type: Session
        last_tick = (
            select(
                PriceTick.price.label("last_price"),
                PriceTick.ts.label("last_ts"),
                PriceTick.currency.label("last_currency"),
                PriceTick.source.label("last_source"),
            )
            .where(PriceTick.ticker == Watchlist.ticker)
            .order_by(PriceTick.ts.desc())
            .limit(1)
            .lateral()
        )

        stmt = (
            select(
                Watchlist.id,
                Watchlist.ticker,
                Watchlist.is_on,
                Watchlist.poll_seconds,
                Watchlist.last_polled_at,
                last_tick.c.last_price,
                last_tick.c.last_ts,
                last_tick.c.last_currency,
                last_tick.c.last_source,
            )
            .select_from(Watchlist)
            .outerjoin(last_tick, true())
            .order_by(Watchlist.ticker)
        )

        watch = db.execute(stmt).mappings().all()

    return render_template(
        "index.html",
        watch=watch,
        balance=12334,
        balance_currency='RUB',
        poll_tick_seconds=SCHED_TICK_SECONDS,
    )


@app.post("/add")
def add_ticker():
    ticker = (request.form.get("ticker") or "").strip().upper()
    poll_seconds = int((request.form.get("poll_seconds") or "30").strip() or 30)

    if ticker:
        stmt = (
            pg_insert(Watchlist)
            .values(ticker=ticker, is_on=True, poll_seconds=poll_seconds)
            .on_conflict_do_update(
                index_elements=[Watchlist.ticker],
                set_={"is_on": True, "poll_seconds": poll_seconds},
            )
        )
        with SessionLocal() as s:
            s.execute(stmt)
            s.commit()

    return redirect(url_for("index"))


@app.post("/toggle/<int:watch_id>")
def toggle(watch_id: int):
    with SessionLocal() as s:
        s.execute(
            update(Watchlist)
            .where(Watchlist.id == watch_id)
            .values(is_on=~Watchlist.is_on)
        )
        s.commit()
    return redirect(url_for("index"))


@app.post("/set_interval/<int:watch_id>")
def set_interval(watch_id: int):
    poll_seconds = int((request.form.get("poll_seconds") or "30").strip() or 30)
    if poll_seconds < 1:
        poll_seconds = 1

    with SessionLocal() as s:
        s.execute(
            update(Watchlist)
            .where(Watchlist.id == watch_id)
            .values(poll_seconds=poll_seconds)
        )
        s.commit()

    return redirect(url_for("index"))


@app.post("/delete/<int:watch_id>")
def delete_ticker(watch_id: int):
    with SessionLocal() as s:
        s.execute(delete(Watchlist).where(Watchlist.id == watch_id))
        s.commit()
    return redirect(url_for("index"))


@app.get("/metrics")
def metrics():
    with SessionLocal() as s:
        has_any = s.execute(select(func.count(PriceTick.id))).scalar_one()
    if not has_any:
        return ("no metrics yet", 404)

    payload = generate_latest()
    return Response(payload, mimetype=CONTENT_TYPE_LATEST)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=False)
