import os
import json
import time
import asyncio
import uuid
import re
from typing import Dict, Any, List, Optional

import httpx
from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel

# psycopg (бинарные колёса)
import psycopg
from psycopg_pool import AsyncConnectionPool
from psycopg.types.json import Json

# импорт «знаний»
from kb import SYSTEM_PROMPT, COURSE_HINTS, expand_links, rule_suggestions

# -----------------------------
# ENV
# -----------------------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
WEBHOOK_SECRET      = os.getenv("WEBHOOK_SECRET", "")
ADMIN_TOKEN         = os.getenv("ADMIN_TOKEN", "")
DATABASE_URL        = os.getenv("DATABASE_URL", "")
MAIN_CHAT_ID        = int(os.getenv("MAIN_CHAT_ID", "0") or 0)
SUGGESTIONS_CHAT_ID = int(os.getenv("SUGGESTIONS_CHAT_ID", "0") or 0)
OPENAI_API_KEY      = os.getenv("OPENAI_API_KEY", "")
MODEL_NAME          = os.getenv("MODEL_NAME", "gpt-4o-mini")

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

# -----------------------------
# DB (psycopg + async pool)
# -----------------------------
POOL: Optional[AsyncConnectionPool] = None

DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS suggestion_sessions (
        id UUID PRIMARY KEY,
        chat_id BIGINT NOT NULL,
        reply_to BIGINT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        s1 TEXT NOT NULL,
        s2 TEXT NOT NULL,
        s3 TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_suggestion_sessions_created ON suggestion_sessions(created_at)",
    """
    CREATE TABLE IF NOT EXISTS edit_waits (
        key TEXT PRIMARY KEY,
        payload JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
]

async def init_db():
    """Создаём пул и таблицы. Без deprecated open=True — открываем пул отдельно."""
    global POOL
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is empty")
    if POOL is None:
        POOL = AsyncConnectionPool(
            DATABASE_URL,
            min_size=1,
            max_size=5,
            kwargs={"autocommit": True},
        )
        await POOL.open()
        async with POOL.connection() as conn:
            async with conn.cursor() as cur:
                for stmt in DDL_STATEMENTS:
                    await cur.execute(stmt)

async def db_execute(query: str, *args):
    async with POOL.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, args if args else None)

async def db_fetchrow(query: str, *args):
    async with POOL.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, args if args else None)
            return await cur.fetchone()

# -----------------------------
# Telegram helpers
# -----------------------------
async def tg_call(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{TELEGRAM_API}/{method}"
    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.post(url, json=payload)
        if r.status_code >= 400:
            try:
                print("[TG ERROR]", r.status_code, r.text)
            except Exception:
                pass
            r.raise_for_status()
        return r.json()

async def send_message(
    chat_id: int,
    text: str,
    reply_to: Optional[int] = None,
    parse_mode: Optional[str] = None,
    reply_markup: Optional[Dict[str, Any]] = None
):
    payload = {"chat_id": chat_id, "text": text}
    if reply_to:
        payload["reply_to_message_id"] = reply_to
        payload["allow_sending_without_reply"] = True
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return await tg_call("sendMessage", payload)

async def answer_callback_query(callback_query_id: str, text: str = ""):
    return await tg_call("answerCallbackQuery", {"callback_query_id": callback_query_id, "text": text})

async def safe_delete_message(chat_id: int, message_id: int):
    try:
        await tg_call("deleteMessage", {"chat_id": chat_id, "message_id": message_id})
    except Exception as e:
        print("[TG DELETE ERROR]", e)

# -----------------------------
# LLM suggestions (3 варианта) с устойчивым парсером
# -----------------------------
async def llm_suggestions(user_text: str) -> List[str]:
    # если ключа нет — сразу умный фолбэк
    if not OPENAI_API_KEY:
        return rule_suggestions(user_text)

    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content":
                f"Сообщение студента: {user_text}\n"
                f"Контекст курса: {COURSE_HINTS}\n"
                f"Ответь строго JSON-списком из трёх строк без комментариев и преамбул."}
        ],
        "temperature": 0.4,
        "max_tokens": 400
    }

    def _parse_suggestions(raw: str) -> List[str]:
        if not raw:
            return []

        raw = raw.strip()

        # 1) Вытянуть из код-фенса ```json ... ```
        m = re.search(r"```(?:json)?\s*(.*?)```", raw, flags=re.S | re.I)
        if m:
            raw = m.group(1).strip()

        # 2) Если это уже JSON — пробуем распарсить
        if raw and raw[0] in "[{":
            try:
                obj = json.loads(raw)
                if isinstance(obj, dict) and "s" in obj and isinstance(obj["s"], list):
                    return [str(x).strip() for x in obj["s"]][:3]
                if isinstance(obj, list):
                    return [str(x).strip() for x in obj][:3]
            except Exception:
                pass

        # 3) Попытка вырезать первый массив [...] из текста
        i, j = raw.find("["), raw.rfind("]")
        if i != -1 and j != -1 and j > i:
            snippet = raw[i:j+1]
            try:
                arr = json.loads(snippet)
                if isinstance(arr, list):
                    return [str(x).strip() for x in arr][:3]
            except Exception:
                pass

        # 4) Фолбэк: берём первые 3 «строки/буллета»
        lines = []
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            line = re.sub(r"^(\d+[\).\:-]|\-|\*|\•)\s*", "", line)
            lines.append(line)
            if len(lines) >= 3:
                break
        return lines[:3]

    async with httpx.AsyncClient(timeout=40.0) as client:
        try:
            r = await client.post(url, headers=headers, json=payload)
            r.raise_for_status()
            data = r.json()
            raw = (data.get("choices", [{}])[0]
                        .get("message", {})
                        .get("content", "")).strip()
            suggestions = _parse_suggestions(raw)

            # нормализуем до 3 вариантов
            out: List[str] = []
            for s in suggestions[:3]:
                s = str(s).replace("\n\n", "\n").strip()
                out.append(s[:600])  # держим коротко, чтобы всё сообщение <4096
            while len(out) < 3:
                out.append("Уточните, пожалуйста, детали — помогу пошагово.")
            return out[:3]

        except Exception as e:
            try:
                print("[LLM RAW FAIL]", raw[:200] if 'raw' in locals() else "<no content>")
            except Exception:
                pass
            print("[LLM ERROR]", e)
            return rule_suggestions(user_text)

# -----------------------------
# Utils
# -----------------------------
async def gc_db():
    await db_execute("DELETE FROM suggestion_sessions WHERE created_at < now() - interval '24 hours'")
    await db_execute("DELETE FROM edit_waits WHERE created_at < now() - interval '2 hours'")

# нормализация команд: /cmd и /cmd@username
def _norm_cmd(t: str) -> str:
    if not t or not t.startswith("/"):
        return ""
    first = t.strip().split()[0]
    base = first.split("@")[0].lower()
    return base

# -----------------------------
# FastAPI
# -----------------------------
app = FastAPI()
class UpdateModel(BaseModel):
    model_config = {"extra": "allow"}

@app.on_event("startup")
async def on_startup():
    await init_db()
    asyncio.create_task(_gc_loop())

@app.get("/health")
async def health():
    return {"ok": True, "time": time.time()}

@app.get("/initdb")
async def initdb(admin: str):
    if admin != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="forbidden")
    await init_db()
    return {"ok": True}

@app.get("/set_webhook")
async def set_webhook(admin: str):
    if admin != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="forbidden")
    return {"hint": "Вызови /set_webhook_url?admin=ADMIN_TOKEN&url=https://<app>.onrender.com/webhook/<WEBHOOK_SECRET>"}

@app.get("/set_webhook_url")
async def set_webhook_url(admin: str, url: str):
    if admin != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="forbidden")
    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.get(
            f"{TELEGRAM_API}/setWebhook",
            params={"url": url, "allowed_updates": json.dumps(["message", "callback_query"])}
        )
        r.raise_for_status()
        return r.json()

@app.post("/webhook/{secret}")
async def webhook(secret: str, request: Request):
    if secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="forbidden")
    update = await request.json()

    # ----- callback_query (кнопки)
    if "callback_query" in update:
        cbq = update["callback_query"]
        await answer_callback_query(cbq.get("id"), "Ок")
        data_raw = cbq.get("data") or ""

        action = None
        sid = None
        idx = 0
        if data_raw.startswith("s:"):
            # формат: s:<uuid>:<i>
            try:
                _, sid, idx_str = data_raw.split(":")
                idx = int(idx_str)
                action = "send"
            except Exception:
                action = None
        elif data_raw.startswith("e:"):
            # формат: e:<uuid>
            try:
                _, sid = data_raw.split(":")
                action = "edit"
            except Exception:
                action = None

        if not action or not sid:
            return {"ok": True}

        # запомним сообщение с кнопками (чтобы удалить позже)
        curator_chat_id = cbq["message"]["chat"]["id"]
        curator_msg_id  = cbq["message"]["message_id"]

        row = await db_fetchrow(
            "SELECT chat_id, reply_to, s1, s2, s3 FROM suggestion_sessions WHERE id=%s",
            sid
        )
        if not row:
            await safe_delete_message(curator_chat_id, curator_msg_id)
            await send_message(curator_chat_id, "Сессия устарела. Сгенерируйте подсказки заново.")
            return {"ok": True}

        chat_id, reply_to, s1, s2, s3 = row
        suggestions = [s1, s2, s3]

        if action == "send":
            text_to_send = expand_links(suggestions[idx] if 0 <= idx < 3 else suggestions[0])
            await send_message(chat_id, text_to_send, reply_to=reply_to)
            await safe_delete_message(curator_chat_id, curator_msg_id)
            return {"ok": True}

        if action == "edit":
            curator_id = cbq.get("from", {}).get("id")
            wait_key = f"wait:{curator_chat_id}:{curator_id}"
            payload = {"chat_id": chat_id, "reply_to": reply_to}
            await db_execute(
                "INSERT INTO edit_waits(key, payload) VALUES(%s,%s) "
                "ON CONFLICT (key) DO UPDATE SET payload=EXCLUDED.payload, created_at=now()",
                wait_key, Json(payload)
            )
            await safe_delete_message(curator_chat_id, curator_msg_id)
            await send_message(curator_chat_id, "Пришлите одним сообщением текст правки — я отправлю его в рабочий чат реплаем к исходному.")
            return {"ok": True}

        return {"ok": True}

    # ----- обычные сообщения
    msg = update.get("message") or update.get("edited_message")
    if not msg:
        return {"ok": True}

    text = msg.get("text") or msg.get("caption") or ""
    chat = msg.get("chat", {})
    chat_id = chat.get("id")
    from_user = msg.get("from", {})
    is_bot = from_user.get("is_bot", False)

    # команды (понимаем /cmd и /cmd@username)
    cmd = _norm_cmd(text)
    if cmd == "/id":
        await send_message(chat_id, f"chat_id: {chat_id}")
        return {"ok": True}
    if cmd == "/ping":
        await send_message(chat_id, "pong")
        return {"ok": True}
    if cmd == "/help":
        await send_message(chat_id, "Команды: /id, /ping, /help. В рабочем чате студента бот отправит подсказки в чат кураторов.")
        return {"ok": True}

    # приём правки из чата кураторов
    if SUGGESTIONS_CHAT_ID and chat_id == SUGGESTIONS_CHAT_ID and text:
        curator_id = from_user.get("id")
        wait_key = f"wait:{SUGGESTIONS_CHAT_ID}:{curator_id}"
        row = await db_fetchrow("SELECT payload FROM edit_waits WHERE key=%s", wait_key)
        if row:
            payload = row[0]  # psycopg отдаст dict для JSONB
            await db_execute("DELETE FROM edit_waits WHERE key=%s", wait_key)
            await send_message(payload["chat_id"], text, reply_to=payload["reply_to"])
            return {"ok": True}

    # генерируем подсказки для MAIN_CHAT (но не на команды)
    if MAIN_CHAT_ID and chat_id == MAIN_CHAT_ID and not is_bot and text and not cmd:
        orig_message_id = msg.get("message_id")
        sender = from_user.get("first_name") or from_user.get("username") or "Студент"

        suggestions = await llm_suggestions(text)
        sid = uuid.uuid4()
        await db_execute(
            "INSERT INTO suggestion_sessions(id, chat_id, reply_to, s1, s2, s3) VALUES(%s,%s,%s,%s,%s,%s)",
            str(sid), chat_id, orig_message_id, suggestions[0], suggestions[1], suggestions[2]
        )

        sugs_display = [expand_links(s) for s in suggestions]

        # короткий callback_data (<64 байт) — без JSON
        keyboard = {
            "inline_keyboard": [
                [
                    {"text": "Отправить 1", "callback_data": f"s:{sid}:0"},
                    {"text": "Отправить 2", "callback_data": f"s:{sid}:1"},
                    {"text": "Отправить 3", "callback_data": f"s:{sid}:2"},
                ],
                [
                    {"text": "Отправить с правкой", "callback_data": f"e:{sid}"},
                ]
            ]
        }

        preview = (
            f"Новое сообщение в рабочем чате от {sender}:\n\n"
            f"{text[:400]}" + ("…" if len(text) > 400 else "") +
            "\n\nВарианты:\n1) " + sugs_display[0] + "\n\n2) " + sugs_display[1] + "\n\n3) " + sugs_display[2]
        )

        target_chat = SUGGESTIONS_CHAT_ID or chat_id
        await send_message(target_chat, preview, reply_markup=keyboard)
        return {"ok": True}

    return {"ok": True}

# GC loop
async def _gc_loop():
    while True:
        try:
            await gc_db()
        except Exception as e:
            print("[GC ERROR]", e)
        await asyncio.sleep(300)
