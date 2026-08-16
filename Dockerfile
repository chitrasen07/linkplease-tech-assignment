FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY scripts ./scripts

# Default SQLite location; mount a persistent disk here in production.
ENV DATABASE_URL=sqlite:////data/linkplease.db
RUN mkdir -p /data

EXPOSE 8000

# $PORT is provided by Render/Heroku-style platforms; 8000 is the local default.
# `exec` makes uvicorn PID 1 so it receives SIGTERM and shuts the workers down
# gracefully instead of being killed after the stop timeout.
CMD ["sh", "-c", "exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
