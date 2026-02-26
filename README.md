# Telegram-бот для подсчёта метрик по видео (TPS)

## Что внутри

- **PostgreSQL** (таблицы `videos`, `video_snapshots`)
- **Загрузчик данных** из JSON: `scripts/load.py`
- **Telegram-бот** (aiogram 3): `app/bot.py`
- **LLM (GigaChat)**: превращает текст → JSON-план (не генерирует SQL напрямую)
- **SQL-исполнение**: `asyncpg` (асинхронно)

Структура:
- `sql/init.sql` — создание таблиц
- `data/videos.json` — пример JSON (в задании выдаётся архив с данными)
- `scripts/load.py` — загрузка JSON → БД
- `ai/prompt.txt` — описание схемы + формат JSON-плана
- `ai/parser.py` — вызывает LLM, валидирует JSON-план, собирает безопасный SQL-шаблон, выполняет и возвращает число
- `app/bot.py` — Telegram-бот (polling)

## Переменные окружения

Секреты в репозиторий не кладутся.  
Создай `.env` рядом с `compose.yml` по примеру `.env_example`.

Обязательные:
- `BOT_TOKEN` — токен Telegram-бота (BotFather)
- `GIGACHAT_AUTH_KEY` — ключ авторизации GigaChat (кабинет GigaChat)

Остальные (есть дефолты):
- `GIGACHAT_SCOPE` (по умолчанию `GIGACHAT_API_PERS`)
- `GIGACHAT_MODEL` (по умолчанию `GigaChat`)
- `GIGACHAT_VERIFY_SSL` (по умолчанию `1`)
- `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DB_PORT` (по умолчанию `tps/tps/tps/5432`)
- `JSON_PATH` (по умолчанию `data/videos.json`)
- `SKIP_TRUNCATE` (по умолчанию `0`)

## Запуск одной командой (Docker Compose)

```bash
docker compose up --build
```

Что произойдёт:
1) Поднимется Postgres и выполнит `sql/init.sql`  
2) Сервис `loader` загрузит JSON (`scripts/load.py`) в таблицы  
3) Запустится Telegram-бот (`app/bot.py`)

## Как устроено «текст → SQL»

1) Пользователь пишет вопрос на русском.  
2) LLM (GigaChat) возвращает JSON-план фиксированного формата (см. `ai/prompt.txt`).  
3) Код **не принимает SQL от модели**. Вместо этого:
   - проверяет `source/action/metric`,
   - подставляет параметры в заранее заданные шаблоны,
   - выполняет запрос в PostgreSQL.
4) Бот отвечает **одним числом**.

## Загрузка данных вручную (если нужно)

Если не используешь compose, можно так:
```bash
export DB_HOST=localhost DB_NAME=tps DB_USER=tps DB_PASSWORD=tps DB_PORT=5432
python scripts/load.py
```

(по умолчанию загрузит `data/videos.json`, можно указать `JSON_PATH=/path/to/videos.json`)
