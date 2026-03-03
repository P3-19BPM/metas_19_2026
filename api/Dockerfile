FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app /app/app
COPY sql /app/sql

ENV PYTHONUNBUFFERED=1
ENV APP_DB_PATH=/app/data/metas.db

RUN mkdir -p /app/data

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
