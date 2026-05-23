# pyAlexS3

OpenAlex S3 → DuckDB loader powered by `rich` progress bars.

Reads OpenAlex NDJSON dumps directly from S3 via DuckDB's `httpfs` extension — no downloading required.

## Features

- 🚀 Direct S3 reads via DuckDB `httpfs` — no local downloads
- 🦆 Zero-setup DuckDB loading via `read_json_auto(...)`
- 🎯 Filter by date range (`YYYY-MM-DD`) and by part numbers
- 🔁 Resume from a specific date and part after a failure
- 🔎 Optional SQL-style `WHERE` predicate
- 📊 Optional `rich` progress bar showing batch progress

## Installation

```bash
pip install pyalexs3
```

or with uv:

```bash
uv add pyalexs3
```

Python **3.10+** is required.

## Quick Start

```python
from pyalexs3.core import OpenAlexS3Processor

p = OpenAlexS3Processor(n_workers=4)

for file_batch, rel in p.lazy_load(
    obj_type="works",
    start_date="2025-01-01",
    end_date="2025-03-01",
    columns=["id", "title", "publication_year"],
):
    df = rel.df()
    print(df.head())
```

## Filter with WHERE clause

```python
for file_batch, rel in p.lazy_load(
    obj_type="works",
    start_date="2025-01-01",
    end_date="2025-03-01",
    columns=["id", "title", "publication_year"],
    where_clause="title IS NOT NULL AND language='en'",
):
    df = rel.df()
```

## Resume After Failure

If your pipeline fails midway, resume from a specific date and part number:

```python
for file_batch, rel in p.lazy_load(
    obj_type="works",
    start_date="2025-01-01",
    end_date="2025-03-01",
    resume_from="2025-01-15/5",  # skip everything before 2025-01-15 part 5
):
    df = rel.df()
```

## Load Specific Parts Only

```python
for file_batch, rel in p.lazy_load(
    obj_type="works",
    start_date="2025-01-01",
    end_date="2025-01-01",
    parts=[0, 1, 2],  # only load part_000.gz, part_001.gz, part_002.gz
):
    df = rel.df()
```

## Show Progress

```python
p = OpenAlexS3Processor(n_workers=4, show_progress=True)

for file_batch, rel in p.lazy_load(obj_type="works"):
    df = rel.df()
```

## Track Which Files Were Processed

Each `lazy_load` iteration yields both the file batch and the relation:

```python
for file_batch, rel in p.lazy_load(obj_type="works"):
    print(f"Processing: {file_batch}")  # list of S3 keys in this batch
    df = rel.df()
```

## API

### `OpenAlexS3Processor(n_workers=4, **kwargs)`

| Parameter              | Type   | Default | Description                         |
| ---------------------- | ------ | ------- | ----------------------------------- |
| `n_workers`            | `int`  | `4`     | DuckDB thread count                 |
| `show_progress`        | `bool` | `False` | Show rich progress bar              |
| `pragma_show_progress` | `bool` | `False` | Enable DuckDB internal progress bar |

### `lazy_load(...) -> Generator[tuple[list[str], DuckDBPyRelation], None, None]`

| Parameter      | Type                | Default      | Description                                         |
| -------------- | ------------------- | ------------ | --------------------------------------------------- |
| `obj_type`     | `str`               | required     | OpenAlex object type e.g. `works`, `authors`        |
| `columns`      | `list[str] \| None` | `None`       | Columns to select. `None` = all                     |
| `limit`        | `int \| None`       | `None`       | Max records per batch                               |
| `start_date`   | `str \| None`       | `2016-06-24` | Start of date range `YYYY-mm-dd` (inclusive)        |
| `end_date`     | `str \| None`       | today        | End of date range `YYYY-mm-dd` (inclusive)          |
| `parts`        | `list[int] \| None` | `None`       | Specific part numbers to load. `None` = all         |
| `where_clause` | `str \| None`       | `None`       | SQL filter. Do not include `WHERE` keyword          |
| `resume_from`  | `str \| None`       | `None`       | Resume from `YYYY-mm-dd/<part>` e.g. `2025-01-15/5` |
| `batch_size`   | `int`               | `10`         | Number of S3 files per batch                        |

Yields `tuple[list[str], duckdb.DuckDBPyRelation]`:

- `list[str]` — S3 keys in this batch (useful for progress tracking)
- `DuckDBPyRelation` — query the batch with `.df()`, `.arrow()`, `.fetchall()`

### Supported Object Types

`works`, `authors`, `sources`, `institutions`, `topics`, `keywords`, `publishers`, `funders`, `concepts`

## Behavior & Notes

- **No downloads** — data is read directly from S3 via DuckDB `httpfs`. No temp files, no cleanup needed.
- **DuckDB** — installs and loads `httpfs` automatically on init. Sets `PRAGMA threads` to `n_workers`.
- **Object cache** — `PRAGMA enable_object_cache=true` is set by default for repeated queries on the same files.
- **S3 auth** — OpenAlex S3 is public. No credentials needed.

## Testing

Dev dependencies include `pytest`.

```bash
uv sync --extra dev
uv run pytest -q
```

Tests mock the S3 client directly using `unittest.mock` to test the file listing and filtering logic without hitting real S3.

## Development

- Source layout: `src/pyalexs3/`
- Typed package marker: `src/pyalexs3/py.typed`

## License

MIT © EurekAI

## Citation

If you are using this for research purposes please use this BibTeX for citation:

```bibtex
@misc{pyalexs32025,
    author = {Adityam Ghosh},
    title = {pyalexs3},
    howpublished = {\url{https://github.com/EurekAI-Org/pyalexs3}},
    year = {2025},
    note = {[Accessed 09-10-2025]},
}
```

