from setuptools import setup, Extension, Command
import subprocess
import sys


# Usage:
#   python setup.py build_protos
#   python setup.py build_ext --inplace


class BuildProtos(Command):
    description = "genera los módulos Python de gRPC a partir de los .proto"
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        subprocess.check_call([
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            "-I",
            ".",
            "--python_out=.",
            "--grpc_python_out=.",
            "protos/p2p_storage.proto",
            "protos/membership.proto",
        ])


fast_rabin_module = Extension(
    "core.fast_rabin",
    sources=["core/fast_rabin.c"],
    extra_compile_args=["-O3"],
)

setup(
    name="backup-p2p",
    version="0.1.0",
    description="Sistema de backup con deduplicación y almacenamiento P2P",
    ext_modules=[fast_rabin_module],
    cmdclass={
        "build_protos": BuildProtos,
    },
)
