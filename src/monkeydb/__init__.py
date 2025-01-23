from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("monkeydb")
except PackageNotFoundError:
    __version__ = "unknown"

from monkeydb import MonkeyDB