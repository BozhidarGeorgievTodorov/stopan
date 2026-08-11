# Desarrollo de Stopan

Este documento resume cómo preparar un entorno de desarrollo del árbol actual del proyecto.

## Requisitos

Stopan requiere Python `>=3.11`.

Dependencias principales en `requirements.txt`:

```text
blake3
grpcio
protobuf
PyYAML
zstandard
cryptography
zfec
```

Dependencias de build en `requirements-build.txt`:

```text
grpcio-tools
setuptools
```

## Entorno editable

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

La instalación editable usa `setup.py`, que declara el paquete `stopan`, el entry point `stopan=stopan.cli.root:main`, los `.proto` y la extensión C `stopan.chunking.fast_rabin`.

## Extensión C

```bash
python setup.py build_ext --inplace
```

## Protobuf

```bash
pip install -r requirements-build.txt
python -m grpc_tools.protoc \
  -Isrc \
  --python_out=src \
  --grpc_python_out=src \
  src/stopan/protos/p2p_storage.proto \
  src/stopan/protos/membership.proto
```

Los `.proto` se versionan. Los módulos `*_pb2.py` y `*_pb2_grpc.py` se regeneran en build.

## Comandos desde el árbol

Usa `python -m stopan` desde el árbol o el entry point `stopan` si has instalado en modo editable.

```bash
python -m stopan --help
python -m stopan config example --out local.example.yaml
```

Los comandos operativos buscan `/etc/stopan/stopan.yaml` por defecto. En desarrollo conviene crear y pasar un YAML local con `--config`:

```bash
python -m stopan init node \
  --config local.stopan.yaml \
  --advertise-addr localhost:50051 \
  --token development \
  --identity-file .stopan/state/node_id.txt \
  --catalog-file .stopan/state/catalog.sqlite \
  --local-chunk-dir .stopan/data/chunks \
  --custody-dir .stopan/custody
```

## Validaciones útiles

Comprobar sintaxis Python sin importar dependencias externas:

```bash
python -m compileall src/stopan
```

Validar configuración:

```bash
python -m stopan config validate local.stopan.yaml
```

Consultar estado de nodo:

```bash
python -m stopan node status --config local.stopan.yaml
```

## Docker

Levantar el clúster demo:

```bash
docker compose up --build
```

Construir el paquete Debian desde Docker:

```bash
docker build --target deb-builder -t stopan-deb .
```

## Empaquetado Debian

`debian/rules` crea un entorno de build, regenera protobuf e instala Stopan en `/opt/stopan/venv` dentro del paquete.

El paquete instala el wrapper `/usr/bin/stopan`, las unidades systemd, los scripts de jobs y los ejemplos de configuración.

Conviene mantener versionado `packaging/examples/stopan.yaml`, porque el empaquetado lo instala como ejemplo de configuración principal.
