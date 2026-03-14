FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir grpcio grpcio-tools blake3 zstandard

COPY setup.py /app/
COPY core/ /app/core/
COPY protos/ /app/protos/
COPY main.py /app/
COPY restore.py /app/
COPY replicator.py /app/
COPY store_service.py /app/

RUN python setup.py build_protos

EXPOSE 50051

CMD ["python", "store_service.py"]
