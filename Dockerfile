FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PORT=8181

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY api/requirements.txt /app/api/requirements.txt
RUN pip install -r /app/api/requirements.txt

COPY api /app/api
COPY public /app/public

EXPOSE 8181

CMD ["sh", "-c", "python -m uvicorn api.app.main:app --host 0.0.0.0 --port ${PORT:-8181}"]
