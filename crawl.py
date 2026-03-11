from __future__ import annotations

import os
import re
import json
import time
import hashlib
from pathlib import Path
from collections import deque
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode
from email.utils import parsedate_to_datetime

import requests
from bs4 import BeautifulSoup
import orjson

BASE = "https://gaokao.eol.cn"
ALLOWED_HOSTS = {"gaokao.eol.cn"}
SEEDS = [
    "https://gaokao.eol.cn/",
    "https://gaokao.eol.cn/daxue/",
    "https://gaokao.eol.cn/daxue/mingdan/index_60.shtml",
    "https://gaokao.eol.cn/zhuanye/",
    "https://gaokao.eol.cn/gaokao/",
    "https://gaokao.eol.cn/qjjh/",
    "https://gaokao.eol.cn/gaozhi/zsxx/",
]

MAX_PAGES = int(os.getenv("MAX_PAGES", "2500"))
SLEEP_SECONDS = float(os.getenv("SLEEP_SECONDS", "0.8"))
TIMEOUT = int(os.getenv("TIMEOUT", "25"))

ROOT = Path(".")
LATEST = ROOT / "data" / "latest"
STATE = ROOT / "data" / "state"
HTML_DIR = LATEST / "html"
for p in [LATEST, STATE, HTML_DIR]:
    p.mkdir(parents=True, exist_ok=True)

SEEN_FILE = STATE / "seen.txt"
FRONTIER_FILE = STATE / "frontier.txt"

DROP_QUERY_KEYS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "spm", "from", "source", "share", "sharefrom", "tdsourcetag",
    "timestamp", "_t", "_", "rand"
}
SKIP_SCHEMES = ("javascript:", "mailto:", "tel:", "data:")
SKIP_EXT_RE = re.compile(
    r"\.(?:jpg|jpeg|png|gif|webp|svg|bmp|ico|pdf|doc|docx|xls|xlsx|ppt|pptx|zip|rar|7z|mp3|mp4|avi|wmv|css|js)(?:\?|$)",
    re.I
)
MULTISLASH_RE = re.compile(r"/{2,}")
WHITESPACE_RE = re.compile(r"\s+")
META_CHARSET_RE = re.compile(br'<meta[^>]+charset=["\']?\s*([a-zA-Z0-9\-_]+)', re.I)
META_CT_RE = re.compile(br'<meta[^>]+content=["\'][^"\']*charset=([a-zA-Z0-9\-_]+)', re.I)

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; gaokao-archive-bot/1.1; +https://github.com/)",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
})

def read_lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    return [x.strip() for x in path.read_text(encoding="utf-8").splitlines() if x.strip()]

def write_lines(path: Path, lines: list[str]):
    path.write_text("\n".join(lines), encoding="utf-8")

def text_of(node) -> str:
    if not node:
        return ""
    return WHITESPACE_RE.sub(" ", node.get_text(" ", strip=True)).strip()

def normalize_charset(enc: str | None) -> str | None:
    if not enc:
        return None
    enc = enc.strip().strip(";").strip().lower()
    alias = {
        "gb2312": "gb18030",
        "gbk": "gb18030",
        "x-gbk": "gb18030",
        "utf8": "utf-8",
    }
    return alias.get(enc, enc)

def detect_encoding(resp: requests.Response, raw: bytes) -> str:
    header_enc = None
    ct = resp.headers.get("content-type", "")
    m = re.search(r"charset=([^\s;]+)", ct, re.I)
    if m:
        header_enc = normalize_charset(m.group(1))

    meta_enc = None
    head = raw[:4096]
    m1 = META_CHARSET_RE.search(head)
    m2 = META_CT_RE.search(head)
    if m1:
        meta_enc = normalize_charset(m1.group(1).decode("ascii", errors="ignore"))
    elif m2:
        meta_enc = normalize_charset(m2.group(1).decode("ascii", errors="ignore"))

    if raw.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"

    # requests 默认可能给出 iso-8859-1，这里尽量不用它
    resp_enc = normalize_charset(resp.encoding)
    if resp_enc and resp_enc not in {"iso-8859-1", "latin-1"}:
        return resp_enc
    if header_enc:
        return header_enc
    if meta_enc:
        return meta_enc

    apparent = normalize_charset(getattr(resp, "apparent_encoding", None))
    if apparent:
        return apparent
    return "utf-8"

def decode_html(resp: requests.Response) -> str:
    raw = resp.content
    enc = detect_encoding(resp, raw)
    try:
        return raw.decode(enc, errors="replace")
    except Exception:
        return raw.decode("utf-8", errors="replace")

def classify(url: str, title: str) -> str:
    if "/zhuanye/" in url:
        return "major"
    if "/daxue/" in url:
        return "school"
    if "/qjjh/" in url:
        return "program"
    if "/zhiyuan/" in url or "/gaokao/" in url:
        return "guide"
    if "/news/" in url or "资讯" in title:
        return "news"
    return "page"

def norm_url(url: str | None, base: str) -> str | None:
    if not url:
        return None
    url = url.strip()
    low = url.lower()
    if low.startswith(SKIP_SCHEMES):
        return None

    abs_url = urljoin(base, url)
    p = urlparse(abs_url)
    if p.scheme not in {"http", "https"}:
        return None
    if p.netloc not in ALLOWED_HOSTS:
        return None

    if SKIP_EXT_RE.search(p.path or ""):
        return None

    path = MULTISLASH_RE.sub("/", p.path or "/")
    if not path.startswith("/"):
        path = "/" + path

    q = []
    for k, v in parse_qsl(p.query, keep_blank_values=True):
        if k.lower() in DROP_QUERY_KEYS:
            continue
        q.append((k, v))
    q.sort()
    query = urlencode(q, doseq=True)

    return urlunparse((p.scheme, p.netloc, path, "", query, ""))

def extract_json_ld(soup: BeautifulSoup) -> list[dict]:
    rows = []
    for s in soup.select('script[type="application/ld+json"]'):
        raw = s.get_text(strip=True)
        if not raw:
            continue
        try:
            data = json.loads(raw)
            rows.append(data)
        except Exception:
            rows.append({"raw": raw[:5000]})
    return rows

def extract_tables(soup: BeautifulSoup, page_url: str, page_type: str) -> list[dict]:
    rows_out = []
    seen_hash = set()
    for idx, table in enumerate(soup.select("table")):
        rows = []
        for tr in table.select("tr")[:500]:
            cells = [text_of(td)[:300] for td in tr.select("th,td")]
            if any(cells):
                rows.append(cells)
        if not rows:
            continue
        table_hash = hashlib.sha1(orjson.dumps(rows)).hexdigest()
        if table_hash in seen_hash:
            continue
        seen_hash.add(table_hash)
        rows_out.append({
            "page_url": page_url,
            "page_type": page_type,
            "table_index": idx,
            "table_hash": table_hash,
            "rows": rows
        })
    return rows_out

def extract_main_text(soup: BeautifulSoup) -> str:
    for tag in soup.select("script,style,noscript,svg,iframe,form"):
        tag.decompose()

    candidates = []
    selectors = [
        "article", "main", "#content", "#article", ".content",
        ".article", ".article-content", ".main", ".main-content"
    ]
    for sel in selectors:
        for node in soup.select(sel):
            txt = text_of(node)
            if len(txt) >= 120:
                candidates.append((len(txt), txt))

    if candidates:
        candidates.sort(reverse=True)
        return candidates[0][1][:4000]

    body = soup.body or soup
    txt = text_of(body)
    return txt[:4000]

def save_jsonl(path: Path, rows: list[dict]):
    with path.open("wb") as f:
        for row in rows:
            f.write(orjson.dumps(row, option=orjson.OPT_APPEND_NEWLINE))

seen = set(read_lines(SEEN_FILE))
frontier_raw = read_lines(FRONTIER_FILE)
frontier = deque()
queued = set()

for u in frontier_raw + SEEDS:
    nu = norm_url(u, BASE)
    if nu and nu not in seen and nu not in queued:
        frontier.append(nu)
        queued.add(nu)

pages_rows = []
tables_rows = []
links_rows = []
jsonld_rows = []

processed = 0

while frontier and processed < MAX_PAGES:
    url = frontier.popleft()
    queued.discard(url)

    if url in seen:
        continue

    try:
        resp = session.get(url, timeout=TIMEOUT, allow_redirects=True)
    except Exception:
        seen.add(url)
        continue

    final_url = norm_url(resp.url, BASE) or url
    seen.add(url)
    seen.add(final_url)

    ctype = resp.headers.get("content-type", "")
    if resp.status_code != 200 or "text/html" not in ctype.lower():
        time.sleep(SLEEP_SECONDS)
        continue

    html = decode_html(resp)
    processed += 1

    sha = hashlib.sha1(final_url.encode("utf-8")).hexdigest()
    html_rel = f"data/html/{sha}.html"
    html_file = HTML_DIR / f"{sha}.html"
    html_file.write_text(html, encoding="utf-8", errors="ignore")

    soup = BeautifulSoup(html, "lxml")
    title = text_of(soup.title)
    meta_desc = ""
    desc_tag = soup.find("meta", attrs={"name": "description"})
    if desc_tag and desc_tag.get("content"):
        meta_desc = desc_tag["content"].strip()

    canonical = None
    can = soup.find("link", attrs={"rel": lambda x: x and "canonical" in x})
    if can and can.get("href"):
        canonical = norm_url(can["href"], final_url)

    page_type = classify(final_url, title)
    headings = [text_of(x) for x in soup.select("h1,h2,h3")[:30] if text_of(x)]
    list_items = [text_of(x) for x in soup.select("li")[:120] if text_of(x)]
    text_snippet = extract_main_text(soup)

    page_links = []
    for a in soup.select("a[href]"):
        dst = norm_url(a.get("href"), final_url)
        if not dst:
            continue
        anchor = text_of(a)[:150]
        page_links.append({
            "src": final_url,
            "dst": dst,
            "anchor": anchor
        })
        if dst not in seen and dst not in queued:
            frontier.append(dst)
            queued.add(dst)

    tables = extract_tables(soup, final_url, page_type)
    jld = extract_json_ld(soup)

    pub_time = None
    for selector in [
        'meta[property="article:published_time"]',
        'meta[name="PubDate"]',
        'meta[name="publishdate"]'
    ]:
        node = soup.select_one(selector)
        if node and node.get("content"):
            pub_time = node["content"].strip()
            break

    if not pub_time:
        t = soup.find("time")
        if t:
            pub_time = text_of(t)

    last_modified = resp.headers.get("last-modified")
    if last_modified:
        try:
            last_modified = parsedate_to_datetime(last_modified).isoformat()
        except Exception:
            pass

    pages_rows.append({
        "url": final_url,
        "canonical": canonical,
        "title": title,
        "type": page_type,
        "meta_description": meta_desc,
        "headings": headings,
        "list_items": list_items,
        "text_snippet": text_snippet,
        "html_snapshot": html_rel,
        "status_code": resp.status_code,
        "content_type": ctype,
        "encoding_used": detect_encoding(resp, resp.content),
        "published_time": pub_time,
        "last_modified": last_modified,
    })

    tables_rows.extend(tables)

    for item in jld:
        jsonld_rows.append({
            "page_url": final_url,
            "page_type": page_type,
            "jsonld": item
        })

    links_rows.extend(page_links)
    time.sleep(SLEEP_SECONDS)

save_jsonl(LATEST / "pages.jsonl", pages_rows)
save_jsonl(LATEST / "tables.jsonl", tables_rows)
save_jsonl(LATEST / "links.jsonl", links_rows)
save_jsonl(LATEST / "jsonld.jsonl", jsonld_rows)

write_lines(SEEN_FILE, sorted(seen))
write_lines(FRONTIER_FILE, list(frontier)[:100000])

summary = {
    "processed_this_run": processed,
    "seen_total": len(seen),
    "frontier_remaining": len(frontier),
    "pages_written": len(pages_rows),
    "tables_written": len(tables_rows),
    "links_written": len(links_rows),
    "jsonld_written": len(jsonld_rows),
}
(LATEST / "summary.json").write_text(
    json.dumps(summary, ensure_ascii=False, indent=2),
    encoding="utf-8"
)
