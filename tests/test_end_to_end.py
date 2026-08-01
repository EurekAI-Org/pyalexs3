from unittest.mock import MagicMock

import pytest

from pyalexs3.core import OpenAlexS3Processor


@pytest.fixture
def processor():
    return OpenAlexS3Processor()


@pytest.fixture
def mock_s3(processor):
    def _setup(keys: list[str]):
        mock_pages = [{"Contents": [{"Key": k} for k in keys]}]
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = mock_pages
        mock_client = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator
        processor._OpenAlexS3Processor__s3_client = mock_client
        return processor

    return _setup


# ------------------------------------------------------------------
# __extract_date
# ------------------------------------------------------------------


def test_extract_date_valid(processor):
    extract = processor._OpenAlexS3Processor__extract_date
    assert (
        extract("data/jsonl/works/updated_date=2025-07-05/part_000.gz") == "2025-07-05"
    )


def test_extract_date_missing(processor):
    extract = processor._OpenAlexS3Processor__extract_date
    assert extract("data/jsonl/works/no_date_here/part_000.gz") == ""


# ------------------------------------------------------------------
# __get_batch_files
# ------------------------------------------------------------------


def test_get_batch_files_basic(mock_s3):
    keys = [
        "data/jsonl/works/updated_date=2025-07-05/part_000.gz",
        "data/jsonl/works/updated_date=2025-07-05/part_001.gz",
        "data/jsonl/works/updated_date=2025-07-05/manifest",
    ]
    p = mock_s3(keys)
    get_batch = p._OpenAlexS3Processor__get_batch_files

    batches = list(
        get_batch(
            obj_type="works",
            data_type="jsonl",
            start_date="2025-07-05",
            end_date="2025-07-05",
            batch_sz=10,
        )
    )

    assert len(batches) == 1
    assert len(batches[0]) == 2
    assert all("manifest" not in f for f in batches[0])


def test_get_batch_files_batching(mock_s3):
    keys = [
        "data/jsonl/works/updated_date=2025-07-05/part_000.gz",
        "data/jsonl/works/updated_date=2025-07-05/part_001.gz",
        "data/jsonl/works/updated_date=2025-07-05/part_002.gz",
    ]
    p = mock_s3(keys)
    get_batch = p._OpenAlexS3Processor__get_batch_files

    batches = list(
        get_batch(
            obj_type="works",
            data_type="jsonl",
            start_date="2025-07-05",
            end_date="2025-07-05",
            batch_sz=2,
        )
    )

    assert len(batches) == 2
    assert len(batches[0]) == 2
    assert len(batches[1]) == 1


def test_get_batch_files_date_filter(mock_s3):
    keys = [
        "data/jsonl/works/updated_date=2025-07-04/part_000.gz",
        "data/jsonl/works/updated_date=2025-07-05/part_000.gz",
        "data/jsonl/works/updated_date=2025-07-06/part_000.gz",
    ]
    p = mock_s3(keys)
    get_batch = p._OpenAlexS3Processor__get_batch_files

    batches = list(
        get_batch(
            obj_type="works",
            data_type="jsonl",
            start_date="2025-07-05",
            end_date="2025-07-05",
            batch_sz=10,
        )
    )

    assert len(batches) == 1
    assert len(batches[0]) == 1
    assert "2025-07-05" in batches[0][0]


def test_get_batch_files_parts_filter(mock_s3):
    keys = [
        "data/jsonl/works/updated_date=2025-07-05/part_000.gz",
        "data/jsonl/works/updated_date=2025-07-05/part_001.gz",
        "data/jsonl/works/updated_date=2025-07-05/part_002.gz",
    ]
    p = mock_s3(keys)
    get_batch = p._OpenAlexS3Processor__get_batch_files

    batches = list(
        get_batch(
            obj_type="works",
            data_type="jsonl",
            start_date="2025-07-05",
            end_date="2025-07-05",
            batch_sz=10,
            parts=[0, 2],
        )
    )

    assert len(batches) == 1
    assert len(batches[0]) == 2
    assert all("part_001" not in f for f in batches[0])


def test_get_batch_files_resume_from(mock_s3):
    keys = [
        "data/jsonl/works/updated_date=2025-07-05/part_000.gz",
        "data/jsonl/works/updated_date=2025-07-05/part_001.gz",
        "data/jsonl/works/updated_date=2025-07-05/part_002.gz",
        "data/jsonl/works/updated_date=2025-07-05/part_003.gz",
    ]
    p = mock_s3(keys)
    get_batch = p._OpenAlexS3Processor__get_batch_files

    batches = list(
        get_batch(
            obj_type="works",
            data_type="jsonl",
            start_date="2025-07-05",
            end_date="2025-07-05",
            batch_sz=10,
            resume_from="2025-07-05/2",
        )
    )

    assert len(batches) == 1
    assert len(batches[0]) == 2
    assert all("part_002" in f or "part_003" in f for f in batches[0])


def test_get_batch_files_resume_from_different_date(mock_s3):
    keys = [
        "data/jsonl/works/updated_date=2025-07-04/part_000.gz",
        "data/jsonl/works/updated_date=2025-07-04/part_001.gz",
        "data/jsonl/works/updated_date=2025-07-05/part_000.gz",
        "data/jsonl/works/updated_date=2025-07-05/part_001.gz",
    ]
    p = mock_s3(keys)
    get_batch = p._OpenAlexS3Processor__get_batch_files

    batches = list(
        get_batch(
            obj_type="works",
            data_type="jsonl",
            start_date="2025-07-04",
            end_date="2025-07-05",
            batch_sz=10,
            resume_from="2025-07-05/0",
        )
    )

    assert len(batches) == 1
    assert len(batches[0]) == 2
    assert all("2025-07-05" in f for f in batches[0])


def test_get_batch_files_empty(mock_s3):
    keys = [
        "data/jsonl/works/updated_date=2025-07-04/part_000.gz",
    ]
    p = mock_s3(keys)
    get_batch = p._OpenAlexS3Processor__get_batch_files

    batches = list(
        get_batch(
            obj_type="works",
            data_type="jsonl",
            start_date="2025-07-05",
            end_date="2025-07-05",
            batch_sz=10,
        )
    )

    assert len(batches) == 0
