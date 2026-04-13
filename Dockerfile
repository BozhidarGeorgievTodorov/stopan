FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app:/app/protos

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --no-cache-dir \
    grpcio \
    grpcio-tools \
    blake3 \
    zstandard

COPY setup.py /app/
COPY core/ /app/core/
COPY protos/ /app/protos/
COPY main.py /app/
COPY restore.py /app/
COPY replicator.py /app/
COPY store_service.py /app/
COPY verifier.py /app/

RUN python setup.py build_protos
RUN python setup.py build_ext --inplace && rm -rf build/

EXPOSE 50051

CMD ["python", "store_service.py"]
