FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1

COPY . .

RUN pip install --no-cache-dir \
    aiogram==3.4.1 \
    asyncpg==0.29.0 \
    aiohttp==3.9.5 \
    psycopg2-binary==2.9.9 \
    python-dotenv==1.0.1

CMD ["python", "-m", "app.bot"]
