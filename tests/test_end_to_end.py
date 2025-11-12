import gzip
import json
from io import BytesIO
from pathlib import Path

import boto3
from moto import mock_aws

import pyalexs3.core as core_mod
from pyalexs3.core import OpenAlexS3Processor


@mock_aws
def test_load_table_end_to_end(tmp_path, monkeypatch):
    monkeypatch.setattr(
        core_mod, "WORKS_SCHEMA", {"id": "VARCHAR", "title": "VARCHAR"}, raising=False
    )

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="openalex")

    monkeypatch.setattr(core_mod.boto3, "client", lambda *a, **k: s3, raising=True)

    key = "data/works/updated_date=2025-07-05/part_000.gz"
    rows = [{"id": "W1", "title": "Hello"}, {"id": "W2", "title": "World"}]
    buf = BytesIO()

    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        for r in rows:
            gz.write((json.dumps(r) + "\n").encode("utf-8"))
    body = buf.getvalue()

    s3.put_object(Bucket="openalex", Key=key, Body=body)

    p = OpenAlexS3Processor()
    p.load_table(
        obj_type="works",
        start_date="2025-07-05",
        end_date="2025-07-05",
        download_dir=str(tmp_path),
    )

    rel = p.get_table("works")
    df = rel.df()

    assert list(df.columns) == ["id", "title"]
    assert len(df) == 2
    assert set(df["title"]) == {"Hello", "World"}


def _put_gz_ndjson(s3, bucket: str, key: str, rows: list[dict]):
    buf = BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        for r in rows:
            gz.write((json.dumps(r) + "\n").encode("utf-8"))
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())


@mock_aws
def test_batch_load_table_end_to_end(tmp_path, monkeypatch):
    # Minimal schema for this test
    monkeypatch.setattr(
        core_mod, "WORKS_SCHEMA", {"id": "VARCHAR", "title": "VARCHAR"}, raising=False
    )

    # Fake S3 and bucket
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="openalex")

    # Force your package to use THIS moto client
    monkeypatch.setattr(core_mod.boto3, "client", lambda *a, **k: s3, raising=True)

    # Two parts on the same date to fall within the range
    _put_gz_ndjson(
        s3,
        "openalex",
        "data/works/updated_date=2025-07-05/part_000.gz",
        [{"id": "W1", "title": "Hello"}, {"id": "W2", "title": "World"}],
    )
    _put_gz_ndjson(
        s3,
        "openalex",
        "data/works/updated_date=2025-07-05/part_001.gz",
        [{"id": "W3", "title": "Batch"}],
    )

    p = OpenAlexS3Processor()

    # Force small batches to exercise the batching path
    p.batch_load_table(
        obj_type="works",
        batch_sz=1,  # ensures multiple batches
        start_date="2025-07-05",
        end_date="2025-07-05",
        download_dir=str(tmp_path),
    )

    # Verify cumulative rows in DuckDB table
    df = p.get_table("works").df()
    assert list(df.columns) == ["id", "title"]
    assert len(df) == 3
    assert set(df["title"]) == {"Hello", "World", "Batch"}

    # batch_load_table typically removes the temp dir
    assert not Path(tmp_path).exists() or not any(Path(tmp_path).glob("*"))


@mock_aws
def test_lazy_load_yields_batches(tmp_path, monkeypatch):
    # Minimal schema
    monkeypatch.setattr(
        core_mod, "WORKS_SCHEMA", {"id": "VARCHAR", "title": "VARCHAR"}, raising=False
    )

    # Fake S3 + bucket
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="openalex")

    # Force library to use THIS client
    monkeypatch.setattr(core_mod.boto3, "client", lambda *a, **k: s3, raising=True)

    # Two parts; with batch_sz=1 we expect two separate lazy batches
    _put_gz_ndjson(
        s3,
        "openalex",
        "data/works/updated_date=2025-07-06/part_000.gz",
        [{"id": "W10", "title": "A"}, {"id": "W11", "title": "B"}],
    )
    _put_gz_ndjson(
        s3,
        "openalex",
        "data/works/updated_date=2025-07-06/part_001.gz",
        [{"id": "W12", "title": "C"}],
    )

    p = OpenAlexS3Processor()

    batch_sizes = []
    titles = []

    for rel in p.lazy_load(
        obj_type="works",
        batch_sz=1,  # each part becomes a batch
        cols=["id", "title"],
        start_date="2025-07-06",
        end_date="2025-07-06",
        download_dir=str(tmp_path),
    ):
        df = rel.df()  # materialize this batch
        batch_sizes.append(len(df))
        titles.extend(df["title"].tolist())

    # Expect 2 batches: sizes [2, 1] matching the two parts above
    assert batch_sizes == [2, 1]
    assert set(titles) == {"A", "B", "C"}

    # lazy_load should clean up the temp dir after each yield; after loop it shouldn't remain
    assert not Path(tmp_path).exists() or not any(Path(tmp_path).glob("*"))


@mock_aws
def test_lazy_load_yields_batches_sel_col(tmp_path, monkeypatch):
    # Minimal schema
    monkeypatch.setattr(
        core_mod,
        "WORKS_SCHEMA",
        {
            "id": "VARCHAR",
            "title": "VARCHAR",
            "display_name": "VARCHAR",
            "fwci": "DOUBLE",
        },
        raising=False,
    )

    # Fake S3 + bucket
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="openalex")

    # Force library to use THIS client
    monkeypatch.setattr(core_mod.boto3, "client", lambda *a, **k: s3, raising=True)

    # Two parts; with batch_sz=1 we expect two separate lazy batches
    _put_gz_ndjson(
        s3,
        "openalex",
        "data/works/updated_date=2025-07-06/part_000.gz",
        [
            {"id": "W10", "title": "A", "display_name": "Some A", "fwci": 2.0},
            {"id": "W11", "title": "B", "display_name": "Some B", "fwci": 4.54},
        ],
    )
    _put_gz_ndjson(
        s3,
        "openalex",
        "data/works/updated_date=2025-07-06/part_001.gz",
        [{"id": "W12", "title": "C", "display_name": "Some C", "fwci": 5.2}],
    )

    p = OpenAlexS3Processor()

    batch_sizes = []
    titles = []

    for rel in p.lazy_load(
        obj_type="works",
        batch_sz=1,  # each part becomes a batch
        cols=["id", "title", "fwci"],
        start_date="2025-07-06",
        end_date="2025-07-06",
        download_dir=str(tmp_path),
    ):
        df = rel.df()  # materialize this batch
        batch_sizes.append(len(df))
        titles.extend(df["title"].tolist())

    # Expect 2 batches: sizes [2, 1] matching the two parts above
    assert batch_sizes == [2, 1]
    assert set(titles) == {"A", "B", "C"}

    # lazy_load should clean up the temp dir after each yield; after loop it shouldn't remain
    assert not Path(tmp_path).exists() or not any(Path(tmp_path).glob("*"))
