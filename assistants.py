# assistants.py
"""
Интеграция с OpenAI Assistants API:
- собираем документы из Google Sheets (CSV) → временные файлы;
- создаём/обновляем Vector Store и Assistant (инструмент file_search);
- даём краткий ответ ассистента по базе (assistants_answer).

Требования к окружению (ENV):
  OPENAI_API_KEY
  ASSISTANT_MODEL              (опц., по умолчанию MODEL_NAME)
  ASSISTANT_ID                 (опц., если пусто — создадим и вернём id)
  ASSISTANT_VECTOR_STORE_ID    (опц., если пусто — создадим и вернём id)
  ASSISTANT_INSTRUCTIONS       (опц., короткие инструкции ассистенту)

  KB_CSV_URL                   (CSV Google Sheets для kb_rules)
  TPL_REMOTE_JSON              (JSON map вкладок-шаблонов: {"welcome": "...csv", ...})

Публичные функции:
  - sync_assistant_from_sheets() → dict  # для вызова из FastAPI эндпойнта /assist_sync
  - assistants_answer(query: str) → str   # получить ответ ассистента (2–4 предложения)
"""

import os
import io
import csv
import json
import time
import tempfile
import asyncio
from typing import List, Tuple, Dict

import httpx
from openai import OpenAI


# -------------------- ENV --------------------

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

ASSISTANT_ID = os.getenv("ASSISTANT_ID", "")
ASSISTANT_MODEL = os.getenv("ASSISTANT_MODEL", os.getenv("MODEL_NAME", "gpt-4o-mini"))
ASSISTANT_VECTOR_STORE_ID = os.getenv("ASSISTANT_VECTOR_STORE_ID", "")

ASSISTANT_INSTRUCTIONS = os.getenv(
    "ASSISTANT_INSTRUCTIONS",
    "Ты — куратор программы по современному искусству. "
    "Отвечай КОРОТКО (2–4 предложения), точно и дружелюбно. "
    "Опирайся ТОЛЬКО на приложенные файлы (file_search). "
    "Не выдумывай факты; если данных нет, попроси уточнение."
)

KB_CSV_URL = os.getenv("KB_CSV_URL", "")
TPL_REMOTE_JSON = os.getenv("TPL_REMOTE_JSON", "")


# -------------------- Client (лениво) --------------------

_client: OpenAI | None = None

def _client_get() -> OpenAI:
    """Лениво создаём клиента OpenAI."""
    global _client
    if _client is None:
        if not OPENAI_API_KEY:
            raise RuntimeError("OPENAI_API_KEY is empty")
        _client = OpenAI(api_key=OPENAI_API_KEY)
    return _client


# -------------------- Загрузка CSV из Google Sheets --------------------

async def _fetch_csv(url: str) -> str:
    """
    Тянем CSV с учётом редиректов; убираем BOM при наличии.
    """
    async with httpx.AsyncClient(timeout=60) as c:
        r = await c.get(url, follow_redirects=True)
        r.raise_for_status()
        text = r.text
        if text.startswith("\ufeff"):
            text = text.lstrip("\ufeff")
        return text

async def _build_documents_from_sheets() -> List[Tuple[str, bytes]]:
    """
    Формируем набор текстовых документов для загрузки в Vector Store:
      - kb_rules.txt из KB_CSV_URL
      - tpl_<name>.txt для каждой вкладки из TPL_REMOTE_JSON
    Каждый документ — простой текст (заголовки/разделы), чтобы ответы были цитатными.
    """
    docs: List[Tuple[str, bytes]] = []

    # kb_rules → kb_rules.txt
    if KB_CSV_URL:
        csv_text = await _fetch_csv(KB_CSV_URL)
        reader = csv.DictReader(io.StringIO(csv_text))
        lines = ["# KB Rules (s1/s2)", ""]
        for i, row in enumerate(reader, 1):
            s1 = (row.get("s1") or "").strip()
            s2 = (row.get("s2") or "").strip()
            patt = (row.get("patterns") or "").strip()
            prio = (row.get("priority") or "").strip()
            if not s1 and not s2:
                continue
            lines.append(f"## Rule {i} (priority={prio or '-'})")
            if patt:
                lines.append(f"patterns: {patt}")
            if s1:
                lines.append(f"s1: {s1}")
            if s2:
                lines.append(f"s2: {s2}")
            lines.append("")
        docs.append(("kb_rules.txt", ("\n".join(lines)).encode("utf-8")))

    # templates → tpl_<name>.txt
    mapping = {}
    if TPL_REMOTE_JSON:
        try:
            mapping = json.loads(TPL_REMOTE_JSON)
            if not isinstance(mapping, dict):
                mapping = {}
        except Exception:
            mapping = {}

    for name, url in mapping.items():
        csv_text = await _fetch_csv(url)
        reader = csv.DictReader(io.StringIO(csv_text))
        lines = [f"# Templates: {name}", ""]
        for row in reader:
            slug = (row.get("slug") or "").strip()
            cat  = (row.get("category") or "").strip()
            text = (row.get("text") or "").strip()
            if not text:
                continue
            lines.append(f"## {slug or '(no-slug)'}  [{cat or '-'}]")
            lines.append(text)
            lines.append("")
        docs.append((f"tpl_{name}.txt", ("\n".join(lines)).encode("utf-8")))

    return docs


def _write_tempfiles(docs: List[Tuple[str, bytes]]) -> List[str]:
    """
    Сохраняем документы во временные файлы — SDK любит реальные файлы.
    Возвращаем список путей.
    """
    paths: List[str] = []
    for (name, data) in docs:
        path = os.path.join("/tmp", f"{int(time.time()*1000)}_{name}")
        with open(path, "wb") as f:
            f.write(data)
        paths.append(path)
    return paths


# -------------------- Синхронизация ассистента/вектор-хранилища --------------------

def _sync_openai_vector_store(paths: List[str]) -> Dict[str, str]:
    """
    Синхронная часть (в отдельном треде): создаём/обновляем Vector Store и Assistant.
    Добавлена «страховочная» проверка, что установлен SDK openai >= 1.35 (есть beta.vector_stores).
    """
    client = _client_get()

    # Явная проверка поддержки vector_stores
    beta = getattr(client, "beta", None)
    if beta is None or not hasattr(beta, "vector_stores"):
        import openai as _openai
        raise RuntimeError(
            f"OpenAI SDK too old for vector stores (installed {_openai.__version__}). "
            "Set openai>=1.35,<2 in requirements.txt, then Clear build cache on Render and redeploy."
        )

    # 1) Vector Store
    vs_id = ASSISTANT_VECTOR_STORE_ID
    if not vs_id:
        vs = client.beta.vector_stores.create(name=f"CuratorAssistantKB-{int(time.time())}")
        vs_id = vs.id

    # 2) Загрузка файлов и привязка к Vector Store
    uploaded = 0
    for p in paths:
        with open(p, "rb") as fh:
            f = client.files.create(file=fh, purpose="assistants")
        client.beta.vector_stores.files.create(vector_store_id=vs_id, file_id=f.id)
        uploaded += 1

    # 3) Assistant
    a_id = ASSISTANT_ID
    if not a_id:
        a = client.beta.assistants.create(
            name="Curator Assistant RAG",
            model=ASSISTANT_MODEL,
            instructions=ASSISTANT_INSTRUCTIONS,
            tools=[{"type": "file_search"}],
            tool_resources={"file_search": {"vector_store_ids": [vs_id]}},
        )
        a_id = a.id
    else:
        # обновляем привязки/модель/инструкции
        client.beta.assistants.update(
            assistant_id=a_id,
            model=ASSISTANT_MODEL,
            instructions=ASSISTANT_INSTRUCTIONS,
            tool_resources={"file_search": {"vector_store_ids": [vs_id]}},
        )

    return {"assistant_id": a_id, "vector_store_id": vs_id, "files_uploaded": uploaded}


async def sync_assistant_from_sheets() -> Dict[str, str]:
    """
    Публичная функция: подтягиваем Google Sheets → готовим документы → грузим в OpenAI.
    Вызывать из FastAPI-эндпойнта /assist_sync.
    """
    docs = await _build_documents_from_sheets()
    if not docs:
        return {"ok": False, "error": "no_docs_from_sheets"}
    paths = _write_tempfiles(docs)
    try:
        res = await asyncio.to_thread(_sync_openai_vector_store, paths)
        res["ok"] = True
        return res
    except Exception as e:
        return {"ok": False, "error": str(e)}


# -------------------- Ответ ассистента --------------------

def _assistants_answer_sync(query: str) -> str:
    """
    Синхронный запрос к ассистенту: создаём thread, добавляем сообщение, запускаем run, ждём completion.
    """
    client = _client_get()

    a_id = os.getenv("ASSISTANT_ID", "") or ASSISTANT_ID
    if not a_id:
        raise RuntimeError("ASSISTANT_ID is empty (сначала вызовите /assist_sync и сохраните id в ENV)")

    th = client.beta.threads.create()
    client.beta.threads.messages.create(thread_id=th.id, role="user", content=query)

    run = client.beta.threads.runs.create(
        thread_id=th.id,
        assistant_id=a_id,
        instructions="Отвечай кратко (2–4 предложения) и только по файлам.",
    )

    # Poll результата
    for _ in range(120):  # до ~90 сек
        st = client.beta.threads.runs.retrieve(thread_id=th.id, run_id=run.id)
        if st.status in ("completed", "failed", "cancelled", "expired"):
            break
        time.sleep(0.8)

    if st.status != "completed":
        raise RuntimeError(f"Assistants run status: {st.status}")

    msgs = client.beta.threads.messages.list(thread_id=th.id, order="desc", limit=10)
    for m in msgs.data:
        if m.role == "assistant":
            parts: List[str] = []
            for c in m.content:
                if getattr(c, "type", "") == "text":
                    parts.append(c.text.value)
            if parts:
                return parts[0].strip()

    return ""


async def assistants_answer(query: str) -> str:
    """Асинхронная обёртка над синхронным SDK."""
    return await asyncio.to_thread(_assistants_answer_sync, query)
