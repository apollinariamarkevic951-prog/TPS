import os
import time
import uuid
import requests

_token = None
_token_expires_at = 0


def _get_access_token():
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

    r = requests.post(url, headers=headers, data=data, timeout=30, verify=False)
    if r.status_code != 200:
        return None

    obj = r.json()
    _token = obj.get("access_token")
    _token_expires_at = int(obj.get("expires_at", 0))
    return _token


def ask_llm(prompt, user_text):
    token = _get_access_token()
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
        "max_tokens": 500,
    }

    r = requests.post(url, headers=headers, json=payload, timeout=60, verify=False)
    if r.status_code != 200:
        return None

    obj = r.json()
    return (obj["choices"][0]["message"]["content"] or "")