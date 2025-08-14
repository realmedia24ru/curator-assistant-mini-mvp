import os, re, csv, io, time, httpx
from typing import List, Dict, Any, Optional

SYSTEM_PROMPT = (
    "Ты — куратор онлайн-программы по современному искусству. "
    "Отвечай кратко, конкретно, дружелюбно, без общих слов."
)
COURSE_HINTS = (
    "Ссылки: <правила>, <анкета>, <вводная>, <база_защит>. "
    "Не забывай про #осебе и #ТочкаА, а также про базу защит."
)

LINKS = {
    "<правила>": "https://t.me/c/2471800961/20",
    "<анкета>": "https://forms.gle/mUYXTjswVtxWVpvJA",
    "<вводная>": "https://t.me/c/2471800961/1737",
    "<база_защит>": "https://onstudy.org/courses/baza-zaschit-hudozhnikov-2-0/",
}

def expand_links(txt: str) -> str:
    if not txt:
        return txt
    out = txt
    pats = [
        (r"<\s*правил[а-я]*\s*>", LINKS["<правила>"]),
        (r"<\s*анкет[а-я]*\s*>",  LINKS["<анкета>"]),
        (r"<\s*вводн[а-я]*\s*>",  LINKS["<вводная>"]),
        (r"<\s*баз[аы]\s*[_\-\s]*защит[а-я]*\s*>", LINKS["<база_защит>"]),
    ]
    for pat, url in pats:
        out = re.sub(pat, url, out, flags=re.I)
    for k, v in LINKS.items():
        out = out.replace(k, v)
    return out

KB_CSV_URL  = os.getenv("KB_CSV_URL", "")
KB_CSV_PATH = os.getenv("KB_CSV_PATH", "kb/kb_rules.csv")

_ACK = [
    "спасибо","спасиб","ок","окей","понял","поняла","ага","угу",
    "класс","супер","отлично","ok","thx","thanks"
]
def _is_ack(text: str) -> bool:
    t = (text or "").lower()
    return ("?" not in t) and any(w in t for w in _ACK) and len(t) <= 120

def _parse_patterns(cell: str):
    if not cell:
        return []
    parts = re.split(r"[;|]", cell)
    out=[]
    for p in parts:
        p = p.strip()
        if not p:
            continue
        if p == "*":
            out.append(("star", "*"))
            continue
        if p.lower().startswith("re:"):
            try:
                rgx = re.compile(p[3:], re.I | re.S)
                out.append(("re", rgx))
            except re.error:
                pass
        else:
            out.append(("str", p.lower()))
    return out

_rows: List[Dict[str, Any]] = []
_loaded_at: float = 0.0

def _load_rows_from_text(text: str) -> List[Dict[str, Any]]:
    if text.startswith("\ufeff"):  # BOM
        text = text.lstrip("\ufeff")
    reader = csv.DictReader(io.StringIO(text))
    rows = []
    for rec in reader:
        pat = _parse_patterns((rec.get("patterns") or "").strip())
        s1  = (rec.get("s1") or "").strip()
        s2  = (rec.get("s2") or "").strip()
        pr  = (rec.get("priority") or "0").strip()
        try:
            pr = int(pr)
        except:
            pr = 0
        if not pat or (not s1 and not s2):
            continue
        rows.append({"patterns": pat, "s1": s1, "s2": s2, "priority": pr})
    rows.sort(key=lambda r: r["priority"], reverse=True)
    return rows

async def reload_kb():
    """Горячо перезагружает правила из Google Sheets CSV или локального файла."""
    global _rows, _loaded_at
    text = ""
    src  = ""
    if KB_CSV_URL:
        src = KB_CSV_URL
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.get(KB_CSV_URL, follow_redirects=True)
            r.raise_for_status()
            text = r.text
    else:
        src = KB_CSV_PATH
        with open(KB_CSV_PATH, "r", encoding="utf-8") as f:
            text = f.read()

    _rows = _load_rows_from_text(text)
    _loaded_at = time.time()
    return {"ok": True, "rows": len(_rows), "src": src, "loaded_at": _loaded_at}

def _score(text: str, row: Dict[str, Any]) -> Optional[int]:
    t = (text or "")
    tl = t.lower()
    hit = False
    sc  = 0
    for kind, patt in row["patterns"]:
        if kind == "star":
            hit = True
            sc  = max(sc, 1)
        elif kind == "str":
            if patt in tl:
                hit = True
                sc = max(sc, 10)
        elif kind == "re":
            if patt.search(t):
                hit = True
                sc = max(sc, 20)
    return (sc + row["priority"]) if hit else None

def _fallback_pair() -> List[str]:
    return [
        "Подскажите, к какому модулю относится вопрос — помогу быстро сориентироваться.",
        "✅ Мини-план: 1) <вводная>, 2) <анкета>, 3) #ТочкаА."
    ]

def rule_suggestions(user_text: str) -> List[str]:
    if _is_ack(user_text):
        return [
            "Пожалуйста! Если появятся вопросы — пишите сюда, поможем.",
            "Рада помочь 💜 Возвращайтесь к лекциям и чату, когда будет удобно."
        ]
    if not _rows:
        return _fallback_pair()

    best = None
    best_sc = -10**9
    for r in _rows:
        sc = _score(user_text, r)
        if sc is not None and sc > best_sc:
            best, best_sc = r, sc

    if best:
        s1 = best.get("s1") or "…"
        s2 = best.get("s2") or "…"
        return [s1, s2]

    return _fallback_pair()

# ---------- Экспорт фрагментов для RAG-варианта ----------
def get_kb_snippets() -> List[Dict[str, str]]:
    """Возвращает список фрагментов из kb_rules для RAG-ответа."""
    out: List[Dict[str, str]] = []
    for i, r in enumerate(_rows):
        s1 = r.get("s1") or ""
        s2 = r.get("s2") or ""
        if s1:
            out.append({"id": f"kb:{i}:s1", "text": expand_links(s1)})
        if s2:
            out.append({"id": f"kb:{i}:s2", "text": expand_links(s2)})
    return out
