FROM python:3.11-slim AS builder

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
COPY requirements-build.txt /app/requirements-build.txt

RUN python -m pip install --no-cache-dir -r /app/requirements.txt \
    && python -m pip install --no-cache-dir -r /app/requirements-build.txt

COPY src/ /app/src/
COPY setup_fast_rabin.py /app/setup_fast_rabin.py

RUN python -m grpc_tools.protoc \
    -I/app/src \
    --python_out=/app/src \
    --grpc_python_out=/app/src \
    /app/src/stopan/protos/p2p_storage.proto \
    /app/src/stopan/protos/membership.proto

RUN python setup_fast_rabin.py build_ext --inplace \
    && rm -rf build/


FROM python:3.11-slim AS runtime

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

COPY requirements.txt /app/requirements.txt

RUN python -m pip install --no-cache-dir -r /app/requirements.txt

COPY --from=builder /app/src /app/src

EXPOSE 50051

CMD ["python", "-m", "stopan", "node"]