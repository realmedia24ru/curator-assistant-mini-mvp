# assistants.py
"""
Интеграция с OpenAI Assistants API:
- тянем Google Sheets (CSV) → формируем текстовые документы → грузим в Vector Store;
- создаём/обновляем Assistant с инструментом file_search;
- выдаём короткий ответ ассистента по базе;
- отдаём статус ассистента/Vector Store.

ENV:
  OPENAI_API_KEY
  ASSISTANT_MODEL
  ASSISTANT_ID
  ASSISTANT_VECTOR_STORE_ID
  ASSISTANT_INSTRUCTIONS
  KB_CSV_URL
  TPL_REMOTE_JSON
"""

import os
import io
import csv
import json
import time
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
    "Отвечай КОРОТКО (2–4 предложения), по делу и дружелюбно. "
    "Опирайся ТОЛЬКО на прикреплённые файлы (file_search). "
    "Не выдумывай фактов; если данных нет — попроси уточнить запрос."
)

KB_CSV_URL = os.getenv("KB_CSV_URL", "")
TPL_REMOTE_JSON = os.getenv("TPL_REMOTE_JSON", "")

# -------------------- Client --------------------

_client: OpenAI | None = None

def _client_get() -> OpenAI:
    global _client
    if _client is None:
        if not OPENAI_API_KEY:
            raise RuntimeError("OPENAI_API_KEY is empty")
        _client = OpenAI(api_key=OPENAI_API_KEY)
    return _client

# -------------------- Google Sheets → docs --------------------

async def _fetch_csv(url: str) -> str:
    async with httpx.AsyncClient(timeout=60) as c:
        r = await c.get(url, follow_redirects=True)
        r.raise_for_status()
        text = r.text
        if text.startswith("\ufeff"):
            text = text.lstrip("\ufeff")
        return text

async def _build_documents_from_sheets() -> List[Tuple[str, bytes]]:
    docs: List[Tuple[str, bytes]] = []

    # kb_rules
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

    # templates
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
    paths: List[str] = []
    for (name, data) in docs:
        path = os.path.join("/tmp", f"{int(time.time()*1000)}_{name}")
        with open(path, "wb") as f:
            f.write(data)
        paths.append(path)
    return paths

# -------------------- Sync VS/Assistant --------------------

def _sync_openai_vector_store(paths: List[str]) -> Dict[str, str]:
    client = _client_get()

    # Проверка версии SDK
    beta = getattr(client, "beta", None)
    if beta is None or not hasattr(beta, "vector_stores"):
        import openai as _openai
        raise RuntimeError(
            f"OpenAI SDK too old for vector stores (installed {_openai.__version__}). "
            "Set openai>=1.35,<2 in requirements.txt, then Clear build cache on Render and redeploy."
        )

    # 1) Vector Store
    vs_id = os.getenv("ASSISTANT_VECTOR_STORE_ID", "") or ASSISTANT_VECTOR_STORE_ID
    if not vs_id:
        vs = client.beta.vector_stores.create(name=f"CuratorAssistantKB-{int(time.time())}")
        vs_id = vs.id

    # 2) Files → VS
    uploaded = 0
    for p in paths:
        with open(p, "rb") as fh:
            f = client.files.create(file=fh, purpose="assistants")
        client.beta.vector_stores.files.create(vector_store_id=vs_id, file_id=f.id)
        uploaded += 1

    # 3) Assistant
    a_id = os.getenv("ASSISTANT_ID", "") or ASSISTANT_ID
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
        client.beta.assistants.update(
            assistant_id=a_id,
            model=ASSISTANT_MODEL,
            instructions=ASSISTANT_INSTRUCTIONS,
            tool_resources={"file_search": {"vector_store_ids": [vs_id]}},
        )

    # Обновим переменные процесса (чтобы сразу работало до перезапуска)
    os.environ["ASSISTANT_ID"] = a_id
    os.environ["ASSISTANT_VECTOR_STORE_ID"] = vs_id
    global ASSISTANT_ID, ASSISTANT_VECTOR_STORE_ID
    ASSISTANT_ID = a_id
    ASSISTANT_VECTOR_STORE_ID = vs_id

    return {"assistant_id": a_id, "vector_store_id": vs_id, "files_uploaded": uploaded}

async def sync_assistant_from_sheets() -> Dict[str, str]:
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

# -------------------- Status / Answer --------------------

def _assistant_status_sync() -> Dict[str, str]:
    client = _client_get()
    a_id = os.getenv("ASSISTANT_ID", "") or ASSISTANT_ID
    vs_id = os.getenv("ASSISTANT_VECTOR_STORE_ID", "") or ASSISTANT_VECTOR_STORE_ID

    out: Dict[str, str | int] = {}
    if a_id:
        a = client.beta.assistants.retrieve(a_id)
        out["assistant_id"] = a_id
        out["model"] = getattr(a, "model", None)
    if vs_id:
        total = 0
        lst = client.beta.vector_stores.files.list(vector_store_id=vs_id, limit=100)
        total += len(lst.data)
        while getattr(lst, "has_more", False):
            last = lst.data[-1].id
            lst = client.beta.vector_stores.files.list(vector_store_id=vs_id, limit=100, after=last)
            total += len(lst.data)
        out["vector_store_id"] = vs_id
        out["files_count"] = total
    return out

async def assistant_status() -> Dict[str, str]:
    return await asyncio.to_thread(_assistant_status_sync)

def _assistants_answer_sync(query: str) -> str:
    client = _client_get()
    a_id = os.getenv("ASSISTANT_ID", "") or ASSISTANT_ID
    if not a_id:
        raise RuntimeError("ASSISTANT_ID is empty (сначала вызовите /assist_sync и сохраните id в ENV)")

    th = client.beta.threads.create()
    client.beta.threads.messages.create(thread_id=th.id, role="user", content=query)
    run = client.beta.threads.runs.create(
        thread_id=th.id, assistant_id=a_id,
        instructions="Отвечай кратко (2–4 предложения) и только по файлам."
    )
    import time as _t
    for _ in range(120):
        st = client.beta.threads.runs.retrieve(thread_id=th.id, run_id=run.id)
        if st.status in ("completed", "failed", "cancelled", "expired"):
            break
        _t.sleep(0.8)
    if st.status != "completed":
        raise RuntimeError(f"Assistants run status: {st.status}")

    msgs = client.beta.threads.messages.list(thread_id=th.id, order="desc", limit=10)
    for m in msgs.data:
        if m.role == "assistant":
            parts = []
            for c in m.content:
                if getattr(c, "type", "") == "text":
                    parts.append(c.text.value)
            if parts:
                return parts[0].strip()
    return ""

async def assistants_answer(query: str) -> str:
    return await asyncio.to_thread(_assistants_answer_sync, query)
