FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir grpcio grpcio-tools

COPY setup.py /app/
COPY core/ /app/core/
COPY protos/ /app/protos/
COPY node_server.py /app/

RUN python setup.py build_protos
RUN python setup.py build_ext --inplace

EXPOSE 50051

CMD ["python", "node_server.py"]
