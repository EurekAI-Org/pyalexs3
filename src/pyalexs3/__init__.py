from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("pyalexs3")
except PackageNotFoundError:
    __version__ = "0.0.0"

from .core import OpenAlexS3Processor

__all__ = ["OpenAlexS3Processor", "__version__"]
