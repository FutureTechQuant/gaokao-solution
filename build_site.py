from __future__ import annotations
import gzip, json
from pathlib import Path

ROOT = Path(".")
LATEST = ROOT / "data" / "latest"
DOCS = ROOT / "docs"
DOCS_DATA = DOCS / "data"
DOCS_DATA.mkdir(parents=True, exist_ok=True)

def read_jsonl(path: Path):
    rows = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows

pages = read_jsonl(LATEST / "pages.jsonl")
tables = read_jsonl(LATEST / "tables.jsonl")
summary = json.loads((LATEST / "summary.json").read_text(encoding="utf-8"))

type_count = {}
for p in pages:
    type_count[p["type"]] = type_count.get(p["type"], 0) + 1

search_index = [{
    "title": p["title"],
    "url": p["url"],
    "type": p["type"],
    "meta_description": p["meta_description"],
    "text_snippet": p["text_snippet"],
    "html_snapshot": p["html_snapshot"],
} for p in pages]

stats = {
    "summary": summary,
    "type_count": type_count,
    "page_count": len(pages),
    "table_count": len(tables),
}

(DOCS_DATA / "search-index.json").write_text(
    json.dumps(search_index, ensure_ascii=False, indent=2),
    encoding="utf-8"
)
(DOCS_DATA / "stats.json").write_text(
    json.dumps(stats, ensure_ascii=False, indent=2),
    encoding="utf-8"
)

for name in ["pages.jsonl", "tables.jsonl", "links.jsonl"]:
    src = LATEST / name
    if src.exists():
        with src.open("rb") as f_in, gzip.open(DOCS_DATA / f"{name}.gz", "wb") as f_out:
            f_out.write(f_in.read())
