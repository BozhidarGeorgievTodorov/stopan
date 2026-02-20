from setuptools import Extension, setup

fast_rabin_module = Extension(
    "fast_rabin",
    sources=["fast_rabin.c"],
    extra_compile_args=["-O3"],
)

setup(
    name="fast-rabin",
    version="0.1.0",
    description="Native CDC boundary finder used by the backup prototype",
    ext_modules=[fast_rabin_module],
)
