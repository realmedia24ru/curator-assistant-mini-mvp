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

AGG_WINDOW          = int(os.getenv("AGG_WINDOW", "8"))  # секунд для склейки сообщений

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
    """Создаём пул и таблицы (без deprecated open=True)."""
    global POOL
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is empty")
    if POOL is None:
        POOL = AsyncConnectionPool(
            DATABASE_URL,
            min_size=1,
            max_size=5,
            open=False,
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
# Helpers
# -----------------------------
def html_escape(s: str) -> str:
    return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

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

async def copy_message(to_chat: int, from_chat: int, msg_id: int):
    try:
        await tg_call("copyMessage", {"chat_id": to_chat, "from_chat_id": from_chat, "message_id": msg_id})
    except Exception as e:
        print("[TG COPY ERROR]", e)

# Определение типа медиа и текстового маркера для агрегатора
def media_marker(msg: Dict[str, Any]) -> Optional[str]:
    mapping = [
        ("photo", "[фото]"),
        ("video", "[видео]"),
        ("animation", "[gif]"),
        ("document", "[документ]"),
        ("audio", "[аудио]"),
        ("voice", "[войс]"),
        ("video_note", "[кружок]"),
        ("sticker", "[стикер]"),
    ]
    for key, label in mapping:
        if key in msg:
            # если это часть альбома — добавим пометку
            if "media_group_id" in msg:
                return f"{label} (альбом)"
            return label
    return None

# -----------------------------
# LLM: один «творческий» вариант
# -----------------------------
async def llm_one_variant(user_text: str) -> str:
    if not OPENAI_API_KEY:
        return "Понимаю запрос. Давайте точечно: опишите, что сейчас в практике важнее — покажу, с чего начать именно вам."

    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content":
                "Ты ментор для современных художников. Отвечай кратко, по делу, дружелюбно. "
                "Дай 1 вариант (2–3 коротких предложения), без плейсхолдеров, но с конкретными шагами/навигацией."},
            {"role": "user", "content": f"Запрос художника: {user_text}"}
        ],
        "temperature": 0.6,
        "max_tokens": 200
    }
    async with httpx.AsyncClient(timeout=40.0) as client:
        try:
            r = await client.post(url, headers=headers, json=payload)
            r.raise_for_status()
            data = r.json()
            raw = (data.get("choices", [{}])[0].get("message", {}).get("content", "")).strip()
            m = re.search(r"```(?:.*?\n)?(.*?)```", raw, flags=re.S)
            if m:
                raw = m.group(1).strip()
            text = raw.split("\n\n")[0].strip()
            if not text:
                text = raw.strip()
            return text[:600]
        except Exception as e:
            print("[LLM ONE ERROR]", e)
            return "Если кратко: сформулируйте ближайшую цель и вопрос к ней — поможем точечно на встрече или здесь."

# Композит: 2 из базы + 1 из LLM
async def compose_suggestions(user_text: str) -> List[str]:
    base = rule_suggestions(user_text)
    llm3 = await llm_one_variant(user_text)
    out = [base[0], base[1], llm3]
    return [s.replace("\n\n","\n").strip()[:600] for s in out]

# -----------------------------
# Склейка сообщений пользователя (debounce)
# -----------------------------
AGG: Dict[str, Dict[str, Any]] = {}  # key -> {texts:[], first_id:int, sender:dict, timer:Task, chat_id}

def _agg_key(chat_id: int, user_id: int) -> str:
    return f"{chat_id}:{user_id}"

async def _agg_fire(key: str):
    await asyncio.sleep(AGG_WINDOW)
    state = AGG.pop(key, None)
    if not state:
        return

    chat_id = state["chat_id"]
    user = state["sender"]
    text_joined = "\n".join([t for t in state["texts"] if t]).strip()
    if not text_joined:
        text_joined = "[сообщение без текста]"

    # Подсказки (2 из базы + 1 из LLM)
    suggestions = await compose_suggestions(text_joined)

    sid = uuid.uuid4()
    await db_execute(
        "INSERT INTO suggestion_sessions(id, chat_id, reply_to, s1, s2, s3) VALUES(%s,%s,%s,%s,%s,%s)",
        str(sid), chat_id, state["first_id"], suggestions[0], suggestions[1], suggestions[2]
    )

    sugs_display = [expand_links(s) for s in suggestions]

    # кликабельное имя
    sender_id = user.get("id")
    sender_name = user.get("first_name") or user.get("username") or "Участник"
    sender_link = f'<a href="tg://user?id={sender_id}">{html_escape(sender_name)}</a>'

    preview = (
        f"Новое сообщение в рабочем чате от {sender_link}:\n\n"
        f"{html_escape(text_joined[:700])}" + ("…" if len(text_joined) > 700 else "") +
        "\n\nВарианты:\n"
        f"1) {html_escape(sugs_display[0])}\n\n"
        f"2) {html_escape(sugs_display[1])}\n\n"
        f"3) {html_escape(sugs_display[2])}"
    )

    # Кнопки: на каждый вариант «Отправить» и «✍ Правка»
    keyboard = {
        "inline_keyboard": [
            [
                {"text": "Отправить 1", "callback_data": f"s:{sid}:0"},
                {"text": "✍ Правка 1",  "callback_data": f"e:{sid}:0"},
            ],
            [
                {"text": "Отправить 2", "callback_data": f"s:{sid}:1"},
                {"text": "✍ Правка 2",  "callback_data": f"e:{sid}:1"},
            ],
            [
                {"text": "Отправить 3", "callback_data": f"s:{sid}:2"},
                {"text": "✍ Правка 3",  "callback_data": f"e:{sid}:2"},
            ],
        ]
    }

    target_chat = SUGGESTIONS_CHAT_ID or chat_id
    await send_message(target_chat, preview, reply_markup=keyboard, parse_mode="HTML")

async def queue_user_piece(chat_id: int, user: Dict[str, Any], message_id: int, piece_text: str):
    key = _agg_key(chat_id, user.get("id"))
    st = AGG.get(key)
    if not st:
        st = {"texts": [], "first_id": message_id, "sender": user, "chat_id": chat_id, "timer": None}
        AGG[key] = st
    st["texts"].append(piece_text)
    # перезапуск таймера
    if st.get("timer"):
        st["timer"].cancel()
    st["timer"] = asyncio.create_task(_agg_fire(key))

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

# нормализация команд: /cmd и /cmd@username
def _norm_cmd(t: str) -> str:
    if not t or not t.startswith("/"):
        return ""
    first = t.strip().split()[0]
    base = first.split("@")[0].lower()
    return base

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
            try:
                _, sid, idx_str = data_raw.split(":")
                idx = int(idx_str)
                action = "send"
            except Exception:
                action = None
        elif data_raw.startswith("e:"):
            try:
                _, sid, idx_str = data_raw.split(":")
                idx = int(idx_str)
                action = "edit"
            except Exception:
                action = None

        if not action or not sid:
            return {"ok": True}

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
            text_to_edit = expand_links(suggestions[idx] if 0 <= idx < 3 else suggestions[0])
            curator_id = cbq.get("from", {}).get("id")
            wait_key = f"wait:{curator_chat_id}:{curator_id}"
            payload = {"chat_id": chat_id, "reply_to": reply_to}
            await db_execute(
                "INSERT INTO edit_waits(key, payload) VALUES(%s,%s) "
                "ON CONFLICT (key) DO UPDATE SET payload=EXCLUDED.payload, created_at=now()",
                wait_key, Json(payload)
            )
            await safe_delete_message(curator_chat_id, curator_msg_id)
            await send_message(
                curator_chat_id,
                text_to_edit,
                reply_markup={"force_reply": True, "input_field_placeholder": "Отредактируйте и отправьте — я перешлю в рабочий чат"}
            )
            return {"ok": True}

        return {"ok": True}

    # ----- обычные сообщения
    msg = update.get("message") or update.get("edited_message")
    if not msg:
        return {"ok": True}

    chat = msg.get("chat", {})
    chat_id = chat.get("id")
    from_user = msg.get("from", {})
    is_bot = from_user.get("is_bot", False)
    message_id = msg.get("message_id")

    # команды
    text = msg.get("text") or msg.get("caption") or ""
    cmd = _norm_cmd(text)
    if cmd == "/id":
        await send_message(chat_id, f"chat_id: {chat_id}")
        return {"ok": True}
    if cmd == "/ping":
        await send_message(chat_id, "pong")
        return {"ok": True}
    if cmd == "/help":
        await send_message(chat_id, "Команды: /id, /ping, /help. В рабочем чате карточки генерятся автоматически.")
        return {"ok": True}

    # приём правки из чата кураторов
    if SUGGESTIONS_CHAT_ID and chat_id == SUGGESTIONS_CHAT_ID and text:
        curator_id = from_user.get("id")
        wait_key = f"wait:{SUGGESTIONS_CHAT_ID}:{curator_id}"
        row = await db_fetchrow("SELECT payload FROM edit_waits WHERE key=%s", wait_key)
        if row:
            payload = row[0]
            await db_execute("DELETE FROM edit_waits WHERE key=%s", wait_key)
            await send_message(payload["chat_id"], text, reply_to=payload["reply_to"])
            return {"ok": True}

    # --- рабочий чат: собираем контент (текст + медиа) и шлём карточку с задержкой
    if MAIN_CHAT_ID and chat_id == MAIN_CHAT_ID and not is_bot:
        # медиа — копируем в кураторский чат и добавляем маркер
        label = media_marker(msg)
        if label and SUGGESTIONS_CHAT_ID:
            await copy_message(SUGGESTIONS_CHAT_ID, chat_id, message_id)
            await queue_user_piece(chat_id, from_user, message_id, label)
            return {"ok": True}

        # обычный текст (или подпись к медиа)
        if text and not cmd:
            await queue_user_piece(chat_id, from_user, message_id, text)
            return {"ok": True}

    return {"ok": True}

# GC loop
async def _gc_loop():
    while True:
        try:
            await db_execute("DELETE FROM suggestion_sessions WHERE created_at < now() - interval '24 hours'")
            await db_execute("DELETE FROM edit_waits WHERE created_at < now() - interval '2 hours'")
        except Exception as e:
            print("[GC ERROR]", e)
        await asyncio.sleep(300)
