import asyncio
import os

from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart
from aiogram.types import Message
from dotenv import load_dotenv

from ai.parser import get_number_from_text

load_dotenv()

dp = Dispatcher()


@dp.message(CommandStart())
async def start(message: Message):
    await message.answer("Привет! Напиши вопрос по статистике видео — отвечу одним числом.")


@dp.message()
async def any_text(message: Message):
    value = await get_number_from_text(message.text or "")
    await message.answer(str(value))


async def main():
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("Нет BOT_TOKEN. Создай .env по примеру .env_example")

    bot = Bot(token=token)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
