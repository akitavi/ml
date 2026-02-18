import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://ml:admin@localhost:5433/ml-admin",
)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=20,          # ↑ больше коннектов
    max_overflow=40,
    pool_recycle=1800,     # защита от stale connections
    echo=False,            # включи True если нужно дебажить SQL
    future=True,
)

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    expire_on_commit=False,
)


class Base(DeclarativeBase):
    pass


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
