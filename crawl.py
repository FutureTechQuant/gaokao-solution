from __future__ import annotations

import os
import re
import json
import time
import hashlib
import unicodedata
from pathlib import Path
from collections import deque
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode
from email.utils import parsedate_to_datetime
from datetime import datetime, date

import requests
from bs4 import BeautifulSoup
import orjson

BASE = "https://gaokao.eol.cn"
ALLOWED_HOSTS = {"gaokao.eol.cn"}

SEEDS = [
    "https://gaokao.eol.cn/",
    "https://gaokao.eol.cn/daxue/",
    "https://gaokao.eol.cn/daxue/mingdan/index_47.shtml",
    "https://gaokao.eol.cn/zhuanye/",
    "https://gaokao.eol.cn/zhiyuan/fenshuxian/",
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

NEWS_CUTOFF = date(2021, 1, 1)

NEWS_URL_HINTS = (
    "/news/", "/zixun/", "/dongtai/", "/gaozhi/zsxx/", "/zhengce/"
)

ARTICLE_SELECTORS = [
    "article",
    "main",
    "#content",
    "#article",
    ".content",
    ".article",
    ".article-content",
    ".main-content",
    ".detail",
    ".details",
    ".TRS_Editor",
    ".editor",
]

DROP_NODE_SELECTORS = (
    "script,style,noscript,svg,iframe,form,header,footer,nav,"
    ".breadcrumb,.share,.related,.recommend,.sidebar,.ad,.ads,.top,.bottom"
)

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; gaokao-archive-bot/2.0; +https://github.com/)",
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
    if "/zhiyuan/fenshuxian/" in url or "分数线" in title:
        return "score"
    if "/zhuanye/" in url:
        return "major"
    if "/daxue/" in url:
        return "school"
    if "/qjjh/" in url:
        return "program"
    if any(x in url for x in NEWS_URL_HINTS) or "动态" in title or "资讯" in title or "政策" in title:
        return "news"
    if "/zhiyuan/" in url or "/gaokao/" in url:
        return "guide"
    return "page"


def norm_url(url: str | None, base: str) -> str | None:
    if not url or not isinstance(url, str):
        return None

    url = unicodedata.normalize("NFKC", url).strip()
    base = unicodedata.normalize("NFKC", base).strip()

    if not url:
        return None
    low = url.lower()
    if low.startswith(SKIP_SCHEMES):
        return None

    try:
        abs_url = urljoin(base, url)
    except Exception:
        return None

    try:
        p = urlparse(abs_url)
    except Exception:
        return None

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

    try:
        return urlunparse((p.scheme, p.netloc, path, "", query, ""))
    except Exception:
        return None


def parse_date_str(value: str | None) -> date | None:
    if not value:
        return None
    value = value.strip()

    patterns = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y.%m.%d",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y年%m月%d日",
    ]
    for fmt in patterns:
        try:
            return datetime.strptime(value, fmt).date()
        except Exception:
            pass

    m = re.search(r"(20\d{2})[-/.年](\d{1,2})[-/.月](\d{1,2})", value)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except Exception:
            return None
    return None


def guess_date_from_url(url: str) -> date | None:
    m = re.search(r"t(20\d{2})(\d{2})(\d{2})_", url)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except Exception:
            return None

    m = re.search(r"/(20\d{2})(\d{2})/", url)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), 1)
        except Exception:
            return None

    return None


def is_recent_news(page_type: str, url: str, published_time: str | None) -> bool:
    news_like = (page_type == "news") or any(x in url for x in NEWS_URL_HINTS)
    if not news_like:
        return True
    d = parse_date_str(published_time) or guess_date_from_url(url)
    if not d:
        return False
    return d >= NEWS_CUTOFF


def extract_article_body(soup: BeautifulSoup) -> str:
    temp = BeautifulSoup(str(soup), "lxml")

    for tag in temp.select(DROP_NODE_SELECTORS):
        tag.decompose()

    candidates = []
    for sel in ARTICLE_SELECTORS:
        for node in temp.select(sel):
            paras = [text_of(p) for p in node.select("p") if len(text_of(p)) >= 12]
            if paras:
                text = "\n".join(dict.fromkeys(paras))
                if len(text) >= 120:
                    candidates.append((len(text), text))

    if not candidates:
        for node in temp.find_all(["div", "section"], limit=400):
            paras = [text_of(p) for p in node.select("p") if len(text_of(p)) >= 12]
            if paras:
                text = "\n".join(dict.fromkeys(paras))
                if len(text) >= 200:
                    candidates.append((len(text), text))

    if not candidates:
        return ""

    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1][:8000]


def extract_main_text(soup: BeautifulSoup, page_type: str) -> str:
    if page_type == "news":
        return extract_article_body(soup)

    temp = BeautifulSoup(str(soup), "lxml")
    for tag in temp.select("script,style,noscript,svg,iframe,form"):
        tag.decompose()

    candidates = []
    selectors = [
        "article", "main", "#content", "#article", ".content",
        ".article", ".article-content", ".main", ".main-content"
    ]
    for sel in selectors:
        for node in temp.select(sel):
            txt = text_of(node)
            if len(txt) >= 120:
                candidates.append((len(txt), txt))

    if candidates:
        candidates.sort(reverse=True)
        return candidates[0][1][:4000]

    txt = text_of(temp.body or temp)
    return txt[:4000]


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
            "rows": rows,
        })
    return rows_out


def save_jsonl(path: Path, rows: list[dict]):
    with path.open("wb") as f:
        for row in rows:
            f.write(orjson.dumps(row, option=orjson.OPT_APPEND_NEWLINE))


def dedupe_records(rows: list[dict], keys: tuple[str, ...]) -> list[dict]:
    out = []
    seen = set()
    for row in rows:
        key = tuple(row.get(k) for k in keys)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def extract_school_name(title: str, body: str) -> str | None:
    title = title.strip()
    candidates = []
    for pat in [
        r"^(.+?(?:大学|学院|学校|职业技术大学|职业学院|高等专科学校))",
        r"(.+?(?:大学|学院|学校|职业技术大学|职业学院|高等专科学校))招生",
        r"(.+?(?:大学|学院|学校|职业技术大学|职业学院|高等专科学校))专业",
        r"(.+?(?:大学|学院|学校|职业技术大学|职业学院|高等专科学校))分数线",
    ]:
        m = re.search(pat, title)
        if m:
            candidates.append(m.group(1).strip())
    if not candidates:
        m = re.search(r"([^\s，。；:：()（）]{2,40}(?:大学|学院|学校|职业技术大学|职业学院|高等专科学校))", body[:800])
        if m:
            candidates.append(m.group(1).strip())
    return candidates[0] if candidates else None


def extract_major_name(title: str, body: str) -> str | None:
    title = title.strip()
    pats = [
        r"^(.+?专业)",
        r"(.+?专业)大学排名",
        r"(.+?专业)录取分数线",
        r"全国(.+?专业)",
    ]
    for pat in pats:
        m = re.search(pat, title)
        if m:
            return m.group(1).strip()
    m = re.search(r"([^\s，。；:：()（）]{2,30}专业)", body[:500])
    if m:
        return m.group(1).strip()
    return None


def extract_years(text: str) -> list[int]:
    years = []
    for y in re.findall(r"\b(20\d{2})\b", text):
        yi = int(y)
        if 2000 <= yi <= 2030:
            years.append(yi)
    seen = set()
    out = []
    for y in years:
        if y not in seen:
            seen.add(y)
            out.append(y)
    return out


def looks_like_school_page(url: str, title: str) -> bool:
    joined = f"{url} {title}"
    return (
        "/daxue/" in url
        or "高校名单" in joined
        or "大学名单" in joined
        or "院校" in joined
        or "大学" in title
        or "学院" in title
    )


def looks_like_major_page(url: str, title: str) -> bool:
    joined = f"{url} {title}"
    return "/zhuanye/" in url or "专业" in joined


def looks_like_score_page(url: str, title: str, text_snippet: str) -> bool:
    joined = f"{url} {title} {text_snippet[:500]}"
    return "分数线" in joined or "/zhiyuan/fenshuxian/" in url


def looks_like_major_score_page(url: str, title: str, text_snippet: str) -> bool:
    joined = f"{title} {text_snippet[:1000]}"
    keys = [
        "专业录取分数线",
        "各专业录取分数线",
        "专业分数线",
        "分专业录取分数线",
        "大学专业录取分数线",
    ]
    return any(k in joined for k in keys)


def table_to_score_rows(table_rows: list[list[str]], page_url: str, title: str) -> list[dict]:
    out = []
    if not table_rows:
        return out

    header = [str(x).strip() for x in table_rows[0]]
    header_join = " ".join(header)
    if not any(k in header_join for k in ["学校", "学校名称", "分数", "投档分", "位次", "批次"]):
        return out

    school_idx = None
    batch_idx = None
    score_idx = None
    rank_idx = None
    avg_idx = None

    for i, h in enumerate(header):
        if school_idx is None and ("学校" in h or "院校" in h):
            school_idx = i
        if batch_idx is None and "批次" in h:
            batch_idx = i
        if score_idx is None and any(k in h for k in ["投档分", "最低分", "分数", "录取分"]):
            score_idx = i
        if rank_idx is None and "位次" in h:
            rank_idx = i
        if avg_idx is None and "平均分" in h:
            avg_idx = i

    if school_idx is None or score_idx is None:
        return out

    for row in table_rows[1:]:
        if school_idx >= len(row) or score_idx >= len(row):
            continue
        school_name = row[school_idx].strip()
        score = row[score_idx].strip()
        if not school_name or not score:
            continue
        out.append({
            "source_url": page_url,
            "page_title": title,
            "school_name": school_name,
            "batch": row[batch_idx].strip() if batch_idx is not None and batch_idx < len(row) else None,
            "score": score,
            "rank": row[rank_idx].strip() if rank_idx is not None and rank_idx < len(row) else None,
            "avg_score": row[avg_idx].strip() if avg_idx is not None and avg_idx < len(row) else None,
        })
    return out


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

schools_rows = []
majors_rows = []
scores_rows = []
major_scores_rows = []
news_rows = []

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

    pub_time = None
    for selector in [
        'meta[property="article:published_time"]',
        'meta[name="PubDate"]',
        'meta[name="publishdate"]',
    ]:
        node = soup.select_one(selector)
        if node and node.get("content"):
            pub_time = node["content"].strip()
            break

    if not pub_time:
        t = soup.find("time")
        if t:
            pub_time = text_of(t)

    if not is_recent_news(page_type, final_url, pub_time):
        time.sleep(SLEEP_SECONDS)
        continue

    text_snippet = extract_main_text(soup, page_type)
    if page_type == "news" and not text_snippet:
        time.sleep(SLEEP_SECONDS)
        continue

    page_links = []
    for a in soup.select("a[href]"):
        dst = norm_url(a.get("href"), final_url)
        if not dst:
            continue
        anchor = text_of(a)[:150]
        page_links.append({
            "src": final_url,
            "dst": dst,
            "anchor": anchor,
        })
        if dst not in seen and dst not in queued:
            frontier.append(dst)
            queued.add(dst)

    tables = extract_tables(soup, final_url, page_type)
    jld = extract_json_ld(soup)

    last_modified = resp.headers.get("last-modified")
    if last_modified:
        try:
            last_modified = parsedate_to_datetime(last_modified).isoformat()
        except Exception:
            pass

    page_row = {
        "url": final_url,
        "canonical": canonical,
        "title": title,
        "type": page_type,
        "meta_description": meta_desc,
        "headings": headings,
        "list_items": [] if page_type == "news" else list_items,
        "text_snippet": text_snippet,
        "html_snapshot": html_rel,
        "status_code": resp.status_code,
        "content_type": ctype,
        "encoding_used": detect_encoding(resp, resp.content),
        "published_time": pub_time,
        "last_modified": last_modified,
    }
    pages_rows.append(page_row)

    tables_rows.extend(tables)
    links_rows.extend(page_links)

    for item in jld:
        jsonld_rows.append({
            "page_url": final_url,
            "page_type": page_type,
            "jsonld": item,
        })

    body_for_structured = f"{meta_desc}\n{text_snippet}"
    school_name = extract_school_name(title, body_for_structured)
    major_name = extract_major_name(title, body_for_structured)
    years = extract_years(f"{title}\n{meta_desc}\n{text_snippet[:1500]}")

    if looks_like_school_page(final_url, title):
        schools_rows.append({
            "url": final_url,
            "title": title,
            "school_name": school_name,
            "published_time": pub_time,
            "years": years,
            "meta_description": meta_desc,
            "body": text_snippet,
            "html_snapshot": html_rel,
        })

    if looks_like_major_page(final_url, title):
        majors_rows.append({
            "url": final_url,
            "title": title,
            "major_name": major_name,
            "published_time": pub_time,
            "years": years,
            "meta_description": meta_desc,
            "body": text_snippet,
            "html_snapshot": html_rel,
        })

    if looks_like_score_page(final_url, title, text_snippet):
        scores_rows.append({
            "url": final_url,
            "title": title,
            "school_name": school_name,
            "published_time": pub_time,
            "years": years,
            "meta_description": meta_desc,
            "body": text_snippet,
            "html_snapshot": html_rel,
        })

    if looks_like_major_score_page(final_url, title, text_snippet):
        major_scores_rows.append({
            "url": final_url,
            "title": title,
            "school_name": school_name,
            "major_name": major_name,
            "published_time": pub_time,
            "years": years,
            "meta_description": meta_desc,
            "body": text_snippet,
            "html_snapshot": html_rel,
        })

    if page_type == "news":
        news_rows.append({
            "url": final_url,
            "title": title,
            "published_time": pub_time,
            "body": text_snippet,
            "html_snapshot": html_rel,
        })

    for tb in tables:
        parsed_rows = table_to_score_rows(tb["rows"], final_url, title)
        if parsed_rows:
            for item in parsed_rows:
                scores_rows.append(item)

    time.sleep(SLEEP_SECONDS)

schools_rows = dedupe_records(schools_rows, ("url",))
majors_rows = dedupe_records(majors_rows, ("url",))
scores_rows = dedupe_records(scores_rows, ("url", "title", "school_name", "score"))
major_scores_rows = dedupe_records(major_scores_rows, ("url",))
news_rows = dedupe_records(news_rows, ("url",))

save_jsonl(LATEST / "pages.jsonl", pages_rows)
save_jsonl(LATEST / "tables.jsonl", tables_rows)
save_jsonl(LATEST / "links.jsonl", links_rows)
save_jsonl(LATEST / "jsonld.jsonl", jsonld_rows)

save_jsonl(LATEST / "schools.jsonl", schools_rows)
save_jsonl(LATEST / "majors.jsonl", majors_rows)
save_jsonl(LATEST / "scores.jsonl", scores_rows)
save_jsonl(LATEST / "major_scores.jsonl", major_scores_rows)
save_jsonl(LATEST / "news.jsonl", news_rows)

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
    "schools_written": len(schools_rows),
    "majors_written": len(majors_rows),
    "scores_written": len(scores_rows),
    "major_scores_written": len(major_scores_rows),
    "news_written": len(news_rows),
}

(LATEST / "summary.json").write_text(
    json.dumps(summary, ensure_ascii=False, indent=2),
    encoding="utf-8"
)
