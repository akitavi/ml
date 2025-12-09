FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONFAULTHANDLER=1

WORKDIR /app

# --- Устанавливаем системные зависимости ---
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc wget curl build-essential \
    && rm -rf /var/lib/apt/lists/*

# --- Кэшируем зависимости ---
COPY requirements.txt .

# Обновляем pip + ставим heavy пакеты из wheel
RUN pip install --upgrade pip \
 && pip install --only-binary=:all: numpy pandas pyarrow \
 && pip install -r requirements.txt

# --- Копируем код ---
COPY . .

# --- создаем non-root пользователя ---
RUN useradd -m appuser
USER appuser

EXPOSE 8010

# --- команда запуска ---
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8010"]

