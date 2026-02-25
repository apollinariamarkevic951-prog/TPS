FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && update-ca-certificates

ENV PYTHONUNBUFFERED=1

COPY . .

RUN pip install --no-cache-dir \
    aiogram==3.4.1 \
    aiohttp \
    asyncpg \
    python-dotenv

CMD ["python", "-m", "app.bot"]