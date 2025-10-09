# pyAlexS3

OpenAlex S3 → DuckDB loader with nice progress bars (powered by `rich`).
It lists, filters, downloads (in parallel), and loads OpenAlex NDJSON dumps into DuckDB—either all at once, in batches, or lazily as an iterator.

# Features

- 🚀 Parallel S3 downloads with a live progress bar

- 🦆 Zero-setup DuckDB loading via read_ndjson_auto(...)

- 🧩 Three loading modes:

    - load_table: one-shot into a DuckDB table

    - batch_load_table: append in batches

    - lazy_load: yield a DuckDB relation per batch (no table needed)

- 🎯 Filter by date range (YYYY-MM-DD) and by part numbers

- 🔎 Optional SQL-style WHERE predicate

- 💾 Persistent or in-memory DuckDB

# Installation
```bash
pip install pyalexs3
```
or with uv
```bash
uv add pyalexs3
```

Python **3.10+** is required.

# Quick start
```python
from pyalexs3.core import OpenAlexS3Processor

p = OpenAlexS3Processor(n_workers=4)
p.load_table(
    obj_type="works",
    start_date="2025-07-05",
    end_date="2025-07-20",
    download_dir="./.cache/oa",
    cols=["id", "title"]
)

table = p.get_table("works")
table.limit(5).show()
```

# Filter with WHERE clause
```python
p.load_table(
    obj_type="works",
    start_date="2025-07-05",
    end_date="2025-07-20",
    download_dir="./.cache/oa",
    cols=["id", "title", "type"],
    where_clause="WHERE title IS NOT NULL AND type='article'"
)
```

# Batching and lazy load
## Append in batches
```python
p.batch_load_table(
    obj_type="works",
    batch_sz=5,  # ~number of S3 objects per batch
    start_date="2025-07-01",
    end_date="2025-07-02",
    cols=["id", "title"],
    download_dir="./.cache/oa",
)

# Everything lands in the same DuckDB table:
p.get_table("works").count("*").show()
```

## Iterate lazily (no table required)
```python
titles = []
for rel in p.lazy_load(
    obj_type="works",
    batch_sz=5,
    start_date="2025-07-01",
    end_date="2025-07-02",
    cols=["id", "title"],
    download_dir="./.cache/oa",
):
    df = rel.df()  # materialize this batch
    titles.extend(df["title"].tolist())
```

# API

`OpenAlexS3Processor(n_workers: int = 4, persist_path: str | None = None)`
 - `n_workers`: number of threads for downloads.
- `persist_path`: if set, uses a persistent DuckDB database file at this path; otherwise an in-memory DB.

- `load_table(...) -> None`
        Downloads all matching files and creates/appends a DuckDB table named after `obj_type`.

        Args:
        - `obj_type`: one of `{"works","authors","sources","institutions","topics","keywords","publishers","funders","geo"}`
        - `cols`: `list[str]` of columns to select (default `*`)
        - `limit`: `int | None` (applied after read)
        - `start_date`, `end_date`: ISO "YYYY-MM-DD" strings (inclusive). If `start_date` is None, it’s inferred from S3; if `end_date` is None, defaults to today.
        - `parts`: `list[int] | None` — specific part numbers (e.g., [0,2]). None = all.
        - `download_dir`: temporary folder for gz files (deleted after load)
        - `where_clause`: SQL predicate like "WHERE title IS NOT NULL"

- `batch_load_table(...) -> None`
        Same args as `load_table`, plus:
            - `batch_sz`: approx. number of S3 objects per batch. Each batch is read and inserted (or CREATE on the first), then temp files are deleted.
- `lazy_load(...) -> Iterator[duckdb.DuckDBPyRelation]`
        Yields one `Relation` per batch. You can `.show()`, `.df()`, or run more SQL. Temp files are removed after each yield.

- `get_table(obj_type: str, cols: list[str] | None = None)` -> duckdb.DuckDBPyRelation
        Convenience accessor to query the created table.
- `s3_obj_types -> list[str]`
        Returns supported OpenAlex object types.


# Behavior & notes

- Progress bars: Per-file totals (from head_object) with per-chunk callbacks.

- Threading: Downloads via ThreadPoolExecutor; exceptions bubble up when futures complete.

- DuckDB: Installs/loads httpfs automatically; sets PRAGMA threads to n_workers.

- Cleanup: download_dir is removed at the end of load_table / each batch in batch_load_table / after each yield in lazy_load.

# Testing
Dev dependencies include `pytest` and `moto[s3]` to mock S3.

```bash
# with uv
uv sync --extra dev
uv run pytest -q
```

Example end-to-end tests:

- Mock S3 with `moto`, upload gzipped NDJSON to `openalex` bucket keys,

- Patch `WORKS_SCHEMA` to a minimal schema for fast runs,

- Run `load_table`, `batch_load_table`, and `lazy_load`, then assert results.

# Development

- Source layout: src/pyalexs3/

- Typed package marker: src/pyalexs3/py.typed

# License
MIT © EurekAI

# Citation
If you are using this for research purpose please use this bibTex for citation:
```
@misc{pyalexs32025,
	author = {Adityam Ghosh},
	title = {pyalexs3},
	howpublished = {\url{https://github.com/EurekAI-Org/pyalexs3}},
	year = {2025},
	note = {[Accessed 09-10-2025]},
}
```