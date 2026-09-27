FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# ca-certificates only: publishing goes through the GitHub API, so no git binary
# is needed in the image.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py upload_data.py archive_format.py ./
RUN mkdir -p /app/HOME

HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD test -d /app/HOME && test -w /app/HOME || exit 1

CMD ["python", "main.py"]
