# Stopan

Sistema de backup distribuido direccionado por contenido.

Stopan permite crear snapshots locales de una carpeta, proteger sus chunks en una red P2P mediante replicación o erasure coding, verificar posteriormente esa protección remota y restaurar snapshots desde el CAS local, desde copias remotas de chunks o reconstruyendo data packs EC.

También puede exportar la metadata del snapshot como un grafo de objetos cifrado y firmado, empaquetarlo y distribuirlo por la red para poder reconstruir la base SQLite si se pierde el nodo origen.

## Tecnologías principales

* **Core:** Python 3
* **Red P2P y RPC:** gRPC y Protocol Buffers (Protobuf)
* **Chunking (CDC):** Algoritmo Rabin (extensión nativa en C) y hashing BLAKE3
* **Almacenamiento:** SQLite para metadata y CAS sobre sistema de ficheros para chunks locales, chunks P2P y shards EC
* **Erasure coding:** zfec sobre data packs post-deduplicación
* **Infraestructura:** Docker y Docker Compose para clústeres P2P con membership SWIM

## Arquitectura y flujo general

1. **Backup local:** Troceado de archivos mediante *Content-Defined Chunking*, deduplicación y guardado en CAS local.
2. **Protección P2P (`push`):** Cálculo de nodos destino mediante HRW / Rendezvous Hashing. El modo por defecto replica chunks completos; `--remote-copies` indica las copias remotas requeridas, la copia local no cuenta como copia remota y el nodo origen se excluye. Como alternativa, `--protection-mode ec` agrupa chunks deduplicados en data packs, los codifica con `--ec-k` data shards y `--ec-m` parity shards, y coloca cada shard en un nodo remoto distinto.
3. **Verificación (`verify`):** Auditoría remota sin descarga de blobs. En modo replicación comprueba chunks completos; en modo EC comprueba la presencia de shards registrados por data pack y marca los packs como `VERIFIED`, `DEGRADED` o `FAILED`.
4. **Restauración (`restore`):** Reconstrucción priorizada y explícita. Busca cada chunk en el CAS local y en el store P2P local del nodo. La recuperación remota se selecciona con `--remote-recovery`: por chunks completos (`replication`), por data packs EC (`ec`), ambas rutas (`auto`) o ninguna (`none`).
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
├── node/         # nodo P2P, membership, storage RPC, shards EC y metadata RPC
├── placement/    # HRW, cluster view y placement_epoch
├── protection/   # política, replicación, erasure coding, push y verify
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
python -m stopan restore 1 --out restore_out --remote-recovery none
```

## Flujo conectado a la red

### Protección por replicación de chunks

Con los nodos levantados, proteger los chunks pendientes en 1 nodo remoto:

```bash
python -m stopan push --membership-seed localhost:50051 --remote-copies 1
```

Auditar la protección remota registrada:

```bash
python -m stopan verify --membership-seed localhost:50051
```

Restaurar permitiendo recuperación remota desde nodos P2P:

```bash
python -m stopan restore 1 \
  --out restored_from_network \
  --remote-recovery replication \
  --membership-seed localhost:50051 \
  --replication-targets 1
```

### Protección por erasure coding

También se puede proteger el contenido agrupando chunks deduplicados en data packs y codificando cada pack con erasure coding. Por ejemplo, en el clúster Docker de 4 nodos, el nodo origen queda excluido y los 3 nodos remotos pueden almacenar un esquema `ec_k=2`, `ec_m=1`:

```bash
python -m stopan push --membership-seed localhost:50051 --protection-mode ec --ec-k 2 --ec-m 1
```

Auditar los shards EC registrados sin descargar blobs:

```bash
python -m stopan verify --protection-mode ec
```

Si se pierde el CAS local, `restore` puede reconstruir chunks desde los data packs EC siempre que queden al menos `ec_k` shards recuperables por pack. Esta ruta debe pedirse de forma explícita:

```bash
python -m stopan restore 1 \
  --out restored_from_ec \
  --remote-recovery ec \
  --membership-seed localhost:50051
```

También existe un modo combinado que primero intenta recuperar chunks completos por replicación y después usa EC solo para los chunks que sigan faltando:

```bash
python -m stopan restore 1 --out restored_auto --remote-recovery auto --membership-seed localhost:50051 --replication-targets 1
```

`ec_m=0` está permitido y significa striping sin redundancia: se generan `ec_k` shards, se necesitan todos para reconstruir el pack y cualquier shard perdido hace que el pack pase a `FAILED`.

> **Nota:** Para probar recuperación desde red, después de hacer `push` se puede apartar o borrar el repositorio local `_data_chunks` y ejecutar `restore` de nuevo. En modo EC se necesitan al menos `ec_k + ec_m` nodos remotos elegibles, porque el nodo origen no almacena sus propios shards.

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

Distribuir el último pack de metadata a la red P2P (1 copia remota del pack):

```bash
python -m stopan metadata push --object-store meta_store --passphrase-file pass.txt --identity-file id.json --membership-seed localhost:50051 --pack-copies 1
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