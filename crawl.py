from __future__ import annotations
import os, re, json, time, hashlib
from pathlib import Path
from collections import deque
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
import orjson

BASE = "https://gaokao.eol.cn"
ALLOWED_HOST = "gaokao.eol.cn"
SEEDS = [
    "https://gaokao.eol.cn/daxue/",
    "https://gaokao.eol.cn/daxue/mingdan/index_60.shtml",
    "https://gaokao.eol.cn/zhuanye/",
    "https://gaokao.eol.cn/gaokao/",
    "https://gaokao.eol.cn/qjjh/",
    "https://gaokao.eol.cn/gaozhi/zsxx/",
]
MAX_PAGES = int(os.getenv("MAX_PAGES", "2500"))
SLEEP_SECONDS = float(os.getenv("SLEEP_SECONDS", "0.8"))

ROOT = Path(".")
LATEST = ROOT / "data" / "latest"
STATE = ROOT / "data" / "state"
HTML_DIR = LATEST / "html"
for p in [LATEST, STATE, HTML_DIR]:
    p.mkdir(parents=True, exist_ok=True)

SEEN_FILE = STATE / "seen.txt"
FRONTIER_FILE = STATE / "frontier.txt"

def read_lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    return [x.strip() for x in path.read_text(encoding="utf-8").splitlines() if x.strip()]

def write_lines(path: Path, lines: list[str]):
    path.write_text("\n".join(lines), encoding="utf-8")

def norm(url: str | None) -> str | None:
    if not url:
        return None
    url = url.strip()
    url = urljoin(BASE, url)
    url = url.split("#", 1)[0]
    p = urlparse(url)
    if p.scheme not in {"http", "https"}:
        return None
    if p.netloc != ALLOWED_HOST:
        return None
    path = re.sub(r"/{2,}", "/", p.path or "/")
    q = f"?{p.query}" if p.query else ""
    return f"{p.scheme}://{p.netloc}{path}{q}"

def text_of(node) -> str:
    if not node:
        return ""
    return re.sub(r"\s+", " ", node.get_text(" ", strip=True)).strip()

def classify(url: str, title: str) -> str:
    if "/zhuanye/" in url:
        return "major"
    if "/daxue/" in url:
        return "school"
    if "/qjjh/" in url:
        return "program"
    if "/gaokao/" in url or "/zhiyuan/" in url:
        return "guide"
    if "/news/" in url or "资讯" in title:
        return "news"
    return "page"

def save_jsonl(path: Path, rows: list[dict]):
    with path.open("wb") as f:
        for row in rows:
            f.write(orjson.dumps(row, option=orjson.OPT_APPEND_NEWLINE))

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; academic-archiver/1.0; +https://github.com/)",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
})

seen = set(read_lines(SEEN_FILE))
frontier = deque(read_lines(FRONTIER_FILE) + [u for u in SEEDS if u not in seen])
queued = set(frontier)

pages_rows = []
tables_rows = []
links_rows = []

processed = 0
while frontier and processed < MAX_PAGES:
    url = frontier.popleft()
    queued.discard(url)
    if not url or url in seen:
        continue

    try:
        r = session.get(url, timeout=30)
    except Exception:
        continue

    seen.add(url)
    processed += 1

    ctype = r.headers.get("content-type", "")
    if r.status_code != 200 or "text/html" not in ctype:
        time.sleep(SLEEP_SECONDS)
        continue

    html = r.text
    sha = hashlib.sha1(url.encode("utf-8")).hexdigest()
    html_file = HTML_DIR / f"{sha}.html"
    html_file.write_text(html, encoding="utf-8", errors="ignore")

    soup = BeautifulSoup(html, "lxml")
    title = text_of(soup.title)
    meta_desc = ""
    desc_tag = soup.find("meta", attrs={"name": "description"})
    if desc_tag and desc_tag.get("content"):
        meta_desc = desc_tag["content"].strip()

    headings = [text_of(x) for x in soup.select("h1,h2,h3")[:20] if text_of(x)]
    list_items = [text_of(x) for x in soup.select("li")[:80] if text_of(x)]
    body_text = text_of(soup.body or soup)[:1200]
    page_type = classify(url, title)

    page_links = []
    for a in soup.select("a[href]"):
        nxt = norm(a.get("href"))
        if not nxt:
            continue
        anchor = text_of(a)[:120]
        page_links.append({"src": url, "dst": nxt, "anchor": anchor})
        if nxt not in seen and nxt not in queued:
            frontier.append(nxt)
            queued.add(nxt)

    for idx, table in enumerate(soup.select("table")):
        rows = []
        for tr in table.select("tr")[:300]:
            cells = [text_of(td)[:200] for td in tr.select("th,td")]
            if any(cells):
                rows.append(cells)
        if rows:
            tables_rows.append({
                "page_url": url,
                "page_type": page_type,
                "table_index": idx,
                "rows": rows
            })

    pages_rows.append({
        "url": url,
        "title": title,
        "type": page_type,
        "meta_description": meta_desc,
        "headings": headings,
        "list_items": list_items,
        "text_snippet": body_text,
        "html_snapshot": f"data/html/{sha}.html",
        "status_code": r.status_code,
    })
    links_rows.extend(page_links)
    time.sleep(SLEEP_SECONDS)

save_jsonl(LATEST / "pages.jsonl", pages_rows)
save_jsonl(LATEST / "tables.jsonl", tables_rows)
save_jsonl(LATEST / "links.jsonl", links_rows)

write_lines(SEEN_FILE, sorted(seen))
write_lines(FRONTIER_FILE, list(frontier)[:50000])

summary = {
    "processed_this_run": processed,
    "seen_total": len(seen),
    "frontier_remaining": len(frontier),
    "pages_written": len(pages_rows),
    "tables_written": len(tables_rows),
    "links_written": len(links_rows),
}
(LATEST / "summary.json").write_text(
    json.dumps(summary, ensure_ascii=False, indent=2),
    encoding="utf-8"
)
