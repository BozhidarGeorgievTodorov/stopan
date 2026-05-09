# Stopan

Sistema de backup distribuido direccionado por contenido.

Stopan permite crear snapshots locales de una carpeta, proteger sus chunks en una red P2P, verificar posteriormente esa protección remota y restaurar snapshots desde el CAS local o desde los nodos remotos.

También puede exportar la metadata del snapshot como un grafo de objetos cifrado y firmado, empaquetarlo y distribuirlo por la red para poder reconstruir la base SQLite si se pierde el nodo origen.

## Tecnologías principales

* **Core:** Python 3
* **Red P2P y RPC:** gRPC y Protocol Buffers (Protobuf)
* **Chunking (CDC):** Algoritmo Rabin (extensión nativa en C) y hashing BLAKE3
* **Almacenamiento:** SQLite para metadata y CAS sobre sistema de ficheros para chunks locales y P2P
* **Infraestructura:** Docker y Docker Compose para clústeres P2P con membership SWIM

## Arquitectura y flujo general

1. **Backup local:** Troceado de archivos mediante *Content-Defined Chunking*, deduplicación y guardado en CAS local.
2. **Protección P2P (`push`):** Cálculo de nodos destino mediante HRW / Rendezvous Hashing y envío exclusivo de chunks faltantes. El replication factor `--rf` indica las copias remotas requeridas; la copia local no cuenta como copia remota y el nodo origen se excluye.
3. **Verificación (`verify`):** Auditoría remota sin descarga de blobs para comprobar si los nodos esperados siguen teniendo los chunks asignados, detectando degradación o pérdida de nodos.
4. **Restauración (`restore`):** Reconstrucción priorizada. Busca cada chunk en el CAS local, luego en el store P2P local del nodo y, finalmente, lo solicita a la red P2P si se ha indicado membership.
5. **Metadata distribuida:** La metadata SQLite local puede exportarse a un grafo de objetos, empaquetarse, firmarse y cifrarse para permitir la recuperación de snapshots ante la pérdida total del nodo de origen.

## Estructura

```text
src/stopan/
├── backup/       # creación de snapshots y workers de backup
├── cas/          # repositorio de chunks direccionado por hash
├── chunking/     # CDC, recetas y planificación de chunks
├── cli/          # interfaz de línea de comandos
├── common/       # utilidades compartidas
├── config/       # modelo, defaults y carga de configuración
├── metadata/     # SQLite, identidad, object graph y packs distribuidos
├── node/         # nodo P2P, membership, storage RPC y metadata RPC
├── placement/    # HRW, cluster view y placement_epoch
├── protection/   # política, push y verify de protección remota
├── protos/       # definiciones protobuf
├── replication/  # coordinación y streaming de ReplicateChunks
├── restore/      # recuperación local/P2P/red y escritura segura
├── rpc/          # opciones y pools gRPC comunes
└── scanning/     # recorrido de árboles de ficheros
```

## Requisitos y compilación

Crear un entorno virtual e instalar las dependencias de ejecución:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Regenerar los módulos Python de protobuf y compilar la extensión C:

```bash
python -m grpc_tools.protoc -Isrc/stopan/protos --python_out=src/stopan/protos --grpc_python_out=src/stopan/protos src/stopan/protos/p2p_storage.proto src/stopan/protos/membership.proto
python setup_fast_rabin.py build_ext --inplace
```

## Configuración y nodos P2P

Generar configuración base local:

```bash
python -m stopan init node --config node.yaml --advertise-addr localhost:50051
```

Levantar nodos P2P con Docker (membership SWIM, CAS propio e identidad persistente):

```bash
docker compose up --build
```

## Flujo solo local

Crear una carpeta de prueba y generar un snapshot:

```bash
mkdir -p test_data/docs && printf "hola mundo\n" > test_data/archivo.txt
python -m stopan backup test_data
```

Restaurar el snapshot 1 en otra carpeta usando solo la metadata y el CAS local:

```bash
python -m stopan restore 1 --out restore_out
```

## Flujo conectado a la red

Con los nodos levantados, proteger los chunks pendientes en 1 nodo remoto:

```bash
python -m stopan push --membership-seed localhost:50051 --rf 1
```

Auditar la protección remota registrada:

```bash
python -m stopan verify --membership-seed localhost:50051
```

Restaurar permitiendo recuperación remota desde nodos P2P:

```bash
python -m stopan restore 1 --out restored_from_network --membership-seed localhost:50051 --rf 1
```

> **Nota:** Para probar recuperación desde red, después de hacer `push` se puede apartar o borrar el repositorio local `_data_chunks` y ejecutar `restore` de nuevo.

## Metadata

Stopan permite separar la operativa de metadatos en flujos estrictamente locales o distribuidos.

### Metadata local / offline

Crear una identidad de metadata:

```bash
python -m stopan metadata identity-create --identity-file id.json --passphrase-file pass.txt
```

Se puede realizar un backup que actualice automáticamente el object graph local y genere un pack:

```bash
python -m stopan backup test_data --metadata-passphrase-file pass.txt --metadata-identity-file id.json --metadata-object-store meta_store --metadata-object-pack
```

Alternativamente, el flujo manual permite exportar el grafo, empaquetarlo e importarlo paso a paso:

```bash
python -m stopan metadata export-graph --object-store meta_store --passphrase-file pass.txt --identity-file id.json
python -m stopan metadata pack-graph --object-store meta_store --passphrase-file pass.txt --identity-file id.json --out latest.stopanmetapack
python -m stopan metadata import-pack latest.stopanmetapack --object-store imported_store --passphrase-file pass.txt --identity-file id.json
python -m stopan metadata import-graph --object-store imported_store --passphrase-file pass.txt
```

### Metadata conectada a la red

Distribuir el último pack de metadata a la red P2P (1 copia remota):

```bash
python -m stopan metadata push --object-store meta_store --passphrase-file pass.txt --identity-file id.json --membership-seed localhost:50051 --rf 1
```

Recuperar metadata desde packs remotos y reconstruir un object store local en un nodo nuevo:

```bash
python -m stopan metadata recover --object-store recovered_store --passphrase-file pass.txt --identity-file id.json --membership-seed localhost:50051
```

## GC de metadata

Stopan incluye comandos de garbage collection para limpiar objetos y packs de metadata que ya no son necesarios (se recomienda usar `--dry-run` primero para verificar qué se eliminará sin modificar el store).

Limpieza local del object store (detecta objetos o packs que no forman parte del estado vivo):

```bash
python -m stopan metadata gc --object-store meta_store --passphrase-file pass.txt --identity-file id.json --dry-run
```

Limpieza del store distribuido de packs (se ejecuta en el nodo que mantiene el store remoto):

```bash
python -m stopan metadata-store-gc --config configs/node1.yaml --dry-run
```

*(Para aplicar la limpieza de forma definitiva, se sustituye el flag `--dry-run` por `--apply`).*