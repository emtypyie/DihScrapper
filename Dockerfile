FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    GIT_COMMIT_NAME="DihScrapper Bot" \
    GIT_COMMIT_EMAIL="dihscrapper@users.noreply.github.com"

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py upload_data.py ./
RUN mkdir -p /app/HOME

HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD test -d /app/HOME/.git || exit 1

CMD ["python", "main.py"]
