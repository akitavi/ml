FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# runtime libs для wheels numpy/pandas/pyarrow
RUN apt-get update && apt-get install -y --no-install-recommends \
    libstdc++6 \
    libgomp1 \
    libssl3 \
    libzstd1 \
    liblz4-1 \
    libsnappy1v5 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# кэш зависимостей
COPY requirements.txt .

RUN pip install --upgrade pip \
    # ставим тяжелые строго из wheels
 && pip install --only-binary=:all: numpy pandas pyarrow \
    # остальные — как обычно (sdist разрешён)
 && pip install -r requirements.txt

COPY . .

RUN useradd -m appuser
USER appuser

EXPOSE 8010
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8010"]
