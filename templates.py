# templates.py
import os, csv, io, json, time, httpx
from typing import Dict, Any

# Читаем JSON-карту имя→CSV-URL из ENV
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

    # распарсим JSON-строку: {"welcome":"url1","ls":"url2",...}
    mapping = json.loads(TPL_REMOTE_JSON)
    async with httpx.AsyncClient(timeout=30) as c:
        for name, url in mapping.items():
            r = await c.get(url)
            r.raise_for_status()
            _load_csv_text(r.text, source=url)
    _LOADED_AT = time.time()
    return {"ok": True, "count": len(_TPL), "loaded_at": _LOADED_AT}

def render_template(slug: str, context: Dict[str, Any]) -> Dict[str, Any]:
    """Возвращает {'text','parse_mode'} или {} если не найден."""
    tpl = _TPL.get(slug)
    if not tpl:
        return {}
    try:
        text = tpl["text"].format(**context)
    except Exception:
        # если не хватает переменных — выводим как есть
        text = tpl["text"]
    return {"text": text, "parse_mode": tpl["parse_mode"]}
