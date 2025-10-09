def test_imports():
    import pyalexs3

    assert hasattr(pyalexs3, "__version__")


def test_parser_constructs():
    from pyalexs3.core import OpenAlexS3Processor

    p = OpenAlexS3Processor()
    assert p is not None
