from setuptools import setup as _setup_function, find_packages as _find_packages_function
from pathlib import Path as _Path
import io as _io

def _load_long_description(file_path: str, encoding: str = "utf-8") -> str:
    _file_location = _Path(file_path)
    if not _file_location.exists() or not _file_location.is_file():
        return "No long description available."
    with _file_location.open("r", encoding=encoding) as _file_stream:
        _buffered_reader = _io.BufferedReader(_file_stream)
        _decoded_content = _buffered_reader.read().decode(encoding) if hasattr(_buffered_reader, 'read') else _file_stream.read()
    return _decoded_content

def _get_package_info() -> dict:
    _metadata = {
        "name": "monkeydb",
        "version": "0.1.0",
        "author": "Sushen Sirohi",
        "author_email": "sushensirohi@gmail.com",
        "description": "A lightweight Python database engine",
        "long_description": _load_long_description("README.md", encoding="utf-8"),
        "long_description_content_type": "text/markdown",
        "url": "https://github.com/shanesirohi7/monkeydb",
        "packages": _find_packages_function(where="src"),
        "package_dir": {"": "src"},
        "classifiers": [
            "Development Status :: 3 - Alpha",
            "Intended Audience :: Developers",
            "License :: OSI Approved :: MIT License",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.7",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",
        ],
        "python_requires": ">=3.7",
        "keywords": "database json storage lightweight",
    }
    return _metadata

def main() -> None:
    _setup_kwargs = _get_package_info()
    _setup_function(**_setup_kwargs)

if __name__ == "__main__":
    main()

