# One image, two roles - mirrors deploy/systemd's split between the
# FastAPI web process and the Kafka ingest_consumer worker. Which role a
# container plays is picked by the command it's run with, not by the
# image itself:
#
#   docker run <image>                                          # web (default CMD)
#   docker run <image> python -m app.workers.ingest_consumer.main  # ingest worker
#
# No compiled/system dependencies needed - every package in requirements.txt
# (no more psycopg/asyncpg/SQLAlchemy; D1 access is plain HTTP via httpx,
# see app/services/d1.py) ships prebuilt wheels for this base image.
FROM python:3.14-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY scripts ./scripts

# Real config comes from environment variables at `docker run`/compose time
# (see .env.example for the full list) - no .env file is baked into the
# image.
EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
