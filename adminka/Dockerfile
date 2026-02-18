FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1


RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY src/ .

RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000


ENV POLL_SECONDS=30


CMD ["gunicorn", "-w", "1", "-b", "0.0.0.0:8000", "--timeout", "120", "app:app"]
