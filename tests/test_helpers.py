from pyalexs3.core import OpenAlexS3Processor


def test_extract_date_and_fmt():
    p = OpenAlexS3Processor()

    extract = p._OpenAlexS3Processor__extract_date
    checkfmt = p._OpenAlexS3Processor__check_date_fmt

    key = "data/works/updated_date=2025-07-05/part_000.gz"
    assert extract(key) == "2025-07-05"

    assert checkfmt("2025-07-05") is True
    assert checkfmt("2025-7-5") is False
    assert checkfmt("not-a-date") is False
