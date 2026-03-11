from __future__ import annotations

import gzip
import json
import shutil
from pathlib import Path

ROOT = Path(".")
LATEST = ROOT / "data" / "latest"
DOCS = ROOT / "docs"
DOCS_DATA = DOCS / "data"
DOCS_HTML = DOCS_DATA / "html"

DOCS.mkdir(parents=True, exist_ok=True)
DOCS_DATA.mkdir(parents=True, exist_ok=True)
DOCS_HTML.mkdir(parents=True, exist_ok=True)


def read_jsonl(path: Path) -> list[dict]:
    rows = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def gzip_copy(src: Path, dst: Path):
    with src.open("rb") as f_in, gzip.open(dst, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)


def safe_unlink_children(dir_path: Path):
    if not dir_path.exists():
        return
    for item in dir_path.iterdir():
        if item.is_file():
            item.unlink()


pages = read_jsonl(LATEST / "pages.jsonl")
tables = read_jsonl(LATEST / "tables.jsonl")
links = read_jsonl(LATEST / "links.jsonl")
jsonld = read_jsonl(LATEST / "jsonld.jsonl")

summary_path = LATEST / "summary.json"
if summary_path.exists():
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
else:
    summary = {
        "processed_this_run": 0,
        "seen_total": 0,
        "frontier_remaining": 0,
        "pages_written": len(pages),
        "tables_written": len(tables),
        "links_written": len(links),
        "jsonld_written": len(jsonld),
    }

type_count: dict[str, int] = {}
for p in pages:
    page_type = p.get("type") or "page"
    type_count[page_type] = type_count.get(page_type, 0) + 1

search_index = []
for p in pages:
    search_index.append({
        "title": p.get("title", ""),
        "url": p.get("url", ""),
        "type": p.get("type", "page"),
        "meta_description": p.get("meta_description", ""),
        "text_snippet": p.get("text_snippet", ""),
        "html_snapshot": p.get("html_snapshot", ""),
        "published_time": p.get("published_time"),
        "encoding_used": p.get("encoding_used"),
    })

stats = {
    "summary": summary,
    "type_count": type_count,
    "page_count": len(pages),
    "table_count": len(tables),
    "link_count": len(links),
    "jsonld_count": len(jsonld),
}

(DOCS_DATA / "search-index.json").write_text(
    json.dumps(search_index, ensure_ascii=False, indent=2),
    encoding="utf-8"
)
(DOCS_DATA / "stats.json").write_text(
    json.dumps(stats, ensure_ascii=False, indent=2),
    encoding="utf-8"
)

# 复制 HTML 快照到 Pages 可发布目录
safe_unlink_children(DOCS_HTML)
src_html = LATEST / "html"
if src_html.exists():
    for f in src_html.glob("*.html"):
        shutil.copy2(f, DOCS_HTML / f.name)

# 压缩导出原始数据
for name in ["pages.jsonl", "tables.jsonl", "links.jsonl", "jsonld.jsonl"]:
    src = LATEST / name
    if src.exists():
        gzip_copy(src, DOCS_DATA / f"{name}.gz")

# 额外输出一个轻量首页数据，便于前端直接渲染
latest_pages = sorted(
    pages,
    key=lambda x: (x.get("published_time") or "", x.get("title") or ""),
    reverse=True
)[:200]

(DOCS_DATA / "latest-pages.json").write_text(
    json.dumps(latest_pages, ensure_ascii=False, indent=2),
    encoding="utf-8"
)

# 避免 Jekyll 干预静态资源路径
(DOCS / ".nojekyll").write_text("", encoding="utf-8")
