FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir \
    aiogram==3.4.1 \
    psycopg2-binary \
    requests \
    python-dotenv

CMD ["python", "-m", "app.bot"]