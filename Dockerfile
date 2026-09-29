FROM python:3.14-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_INDEX_URL=https://pypi.org/simple

WORKDIR /app

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy downloaded wheels and install from local directory
COPY wheels /tmp/dih_wheels
RUN pip install --no-cache-dir --find-links=/tmp/dih_wheels /tmp/dih_wheels/*

# Copy application files
COPY main.py uploader.py archive_format.py ./
RUN mkdir -p /app/HOME

HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD test -d /app/HOME && test -w /app/HOME || exit 1

CMD ["python", "main.py"]