FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONFAULTHANDLER=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        curl \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY src/ .

RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser


EXPOSE 8050

CMD ["gunicorn", \
     "-k", "uvicorn.workers.UvicornWorker", \
     "-w", "1", \
     "-b", "0.0.0.0:8050", \
     "--timeout", "30", \
     "app:app"]
