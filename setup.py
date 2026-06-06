from __future__ import annotations

from pathlib import Path

from setuptools import Extension, find_packages, setup


ROOT = Path(__file__).parent


def read_text(path: str, default: str = "") -> str:
    file_path = ROOT / path
    if not file_path.exists():
        return default
    return file_path.read_text(encoding="utf-8")


setup(
    name="stopan",
    version="1.0.0",
    description=(
        "Stopan: distributed content-addressed backup"
    ),
    long_description=read_text("README.md", default="Stopan distributed backup system."),
    long_description_content_type="text/markdown",
    python_requires=">=3.11",
    package_dir={"": "src"},
    packages=find_packages("src"),
    include_package_data=True,
    package_data={
        "stopan.protos": ["*.proto"],
        "stopan.chunking": ["*.c"],
    },
    install_requires=[
        "blake3>=1.0.8",
        "grpcio>=1.80.0",
        "protobuf>=6.31.1,<7",
        "PyYAML>=6.0.0",
        "zstandard>=0.25.0",
        "cryptography>=42.0.0",
        "zfec>=1.6.0.0",
    ],
    extras_require={
        "dev": [
            "grpcio-tools>=1.80.0",
        ],
    },
    ext_modules=[
        Extension(
            "stopan.chunking.fast_rabin",
            sources=["src/stopan/chunking/fast_rabin.c"],
        )
    ],
    entry_points={
        "console_scripts": [
            "stopan=stopan.cli.root:main",
        ],
    },
)
