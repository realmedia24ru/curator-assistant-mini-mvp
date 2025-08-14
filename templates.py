import os, csv, io, json, time, httpx
from typing import Dict, Any, List

TPL_REMOTE_JSON = os.getenv("TPL_REMOTE_JSON", "")

_TPL: Dict[str, Dict[str, Any]] = {}
_LOADED_AT = 0.0

def _load_csv_text(text: str, source: str):
    if text.startswith("\ufeff"):
        text = text.lstrip("\ufeff")
    reader = csv.DictReader(io.StringIO(text))
    for r in reader:
        slug = (r.get("slug") or "").strip()
        if not slug:
            continue
        _TPL[slug] = {
            "slug": slug,
            "category": (r.get("category") or "").strip(),
            "text": (r.get("text") or "").strip(),
            "parse_mode": (r.get("parse_mode") or "").strip() or None,
            "notes": (r.get("notes") or "").strip(),
            "_source": source,
        }

async def reload_templates():
    """Грузит ВСЕ шаблоны из CSV-URL, перечисленных в TPL_REMOTE_JSON."""
    global _TPL, _LOADED_AT
    _TPL = {}
    if not TPL_REMOTE_JSON:
        _LOADED_AT = time.time()
        return {"ok": True, "count": 0, "loaded_at": _LOADED_AT, "src": None}

    mapping = json.loads(TPL_REMOTE_JSON)  # {"welcome":"url", ...}
    async with httpx.AsyncClient(timeout=30) as c:
        for name, url in mapping.items():
            r = await c.get(url, follow_redirects=True)
            r.raise_for_status()
            _load_csv_text(r.text, source=url)
    _LOADED_AT = time.time()
    return {"ok": True, "count": len(_TPL), "loaded_at": _LOADED_AT}

def render_template(slug: str, context: Dict[str, Any]) -> Dict[str, Any]:
    tpl = _TPL.get(slug)
    if not tpl:
        return {}
    try:
        text = tpl["text"].format(**context)
    except Exception:
        text = tpl["text"]
    return {"text": text, "parse_mode": tpl["parse_mode"]}

# ---------- Экспорт фрагментов для RAG-варианта ----------
def get_template_snippets() -> List[Dict[str, str]]:
    """
    Возвращает список фрагментов из всех шаблонов.
    Мы используем их как справочник для RAG-ответа (только текст).
    """
    out: List[Dict[str, str]] = []
    for slug, rec in _TPL.items():
        txt = (rec.get("text") or "").strip()
        if txt:
            out.append({"id": f"tpl:{slug}", "text": txt})
    return out
