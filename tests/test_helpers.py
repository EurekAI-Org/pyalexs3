from pyalexs3.core import OpenAlexS3Processor


def test_extract_date_and_fmt():
    p = OpenAlexS3Processor()

    extract = p._OpenAlexS3Processor__extract_date  # pyright: ignore

    key = "data/works/jsonl/updated_date=2025-07-05/part_000.gz"
    assert extract(key) == "2025-07-05"
