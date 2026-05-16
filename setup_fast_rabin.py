from setuptools import Extension, find_packages, setup

setup(
    name="stopan-fast-rabin",
    package_dir={"": "src"},
    packages=find_packages("src"),
    ext_modules=[
        Extension(
            "stopan.chunking.fast_rabin",
            sources=["src/stopan/chunking/fast_rabin.c"],
        )
    ],
)