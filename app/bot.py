import asyncio

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart
from aiogram.types import Message

from app.config import get_token
from app.db import fetch_one_int

load_dotenv()

dp = Dispatcher()


@dp.message(CommandStart())
async def start(message: Message):
    await message.answer("Привет! Напиши запрос — я отвечу одним числом.")


@dp.message()
async def any_text(message: Message):
    
    n = fetch_one_int("SELECT COUNT(*) FROM videos;")
    await message.answer(str(n))


async def main():
    bot = Bot(token=get_token())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())