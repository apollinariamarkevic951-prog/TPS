import os
import time
import uuid
import aiohttp
from dotenv import load_dotenv

load_dotenv()

_token: str | None = None
_token_expires_at: int = 0


def _verify_ssl() -> bool:
    
    return os.getenv("GIGACHAT_VERIFY_SSL", "1").strip() not in ("0", "false", "no", "off")


async def _get_access_token(session: aiohttp.ClientSession) -> str | None:
    global _token, _token_expires_at

    if _token and time.time() < (_token_expires_at - 10):
        return _token

    auth_key = os.getenv("GIGACHAT_AUTH_KEY")
    scope = os.getenv("GIGACHAT_SCOPE", "GIGACHAT_API_PERS")

    if not auth_key:
        return None

    url = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
        "RqUID": str(uuid.uuid4()),
        "Authorization": f"Basic {auth_key}",
    }
    data = {"scope": scope}

    async with session.post(url, headers=headers, data=data, timeout=aiohttp.ClientTimeout(total=30), ssl=_verify_ssl()) as r:
        if r.status != 200:
            return None
        obj = await r.json()

    _token = obj.get("access_token")
    _token_expires_at = int(obj.get("expires_at", 0))
    return _token


async def ask_llm(prompt: str, user_text: str) -> str | None:
    async with aiohttp.ClientSession() as session:
        token = await _get_access_token(session)
        if not token:
            return None

        model = os.getenv("GIGACHAT_MODEL", "GigaChat")

        url = "https://gigachat.devices.sberbank.ru/api/v1/chat/completions"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        }
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": prompt},
                {"role": "user", "content": user_text},
            ],
            "temperature": 0.2,
            "max_tokens": 600,
        }

        async with session.post(url, headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=60), ssl=_verify_ssl()) as r:
            if r.status != 200:
                return None
            obj = await r.json()

    return (obj["choices"][0]["message"]["content"] or "")
