# Stopan

Sistema de backup distribuido direccionado por contenido.

Stopan permite crear snapshots locales de una carpeta, proteger sus chunks en una red P2P, verificar posteriormente esa protección remota y restaurar snapshots desde el CAS local o desde los nodos remotos.

## Flujo general

El sistema tiene cuatro operaciones principales:

1. crear snapshots locales de una carpeta;
2. proteger los chunks pendientes en una red P2P;
3. verificar que la protección remota registrada sigue siendo cierta;
4. restaurar un snapshot usando la caché local o la red.

### Backup local

El backup recorre la carpeta origen con `TreeWalker`.

Por cada archivo encontrado, `FileChunker` lo divide en chunks de tamaño variable mediante Content-Defined Chunking. La extensión nativa `fast_rabin` calcula las fronteras de chunk y cada chunk se identifica con BLAKE3.

Antes de escribir un chunk, `ChunkPlanner` decide si debe procesarse o si puede saltarse por fast-path. Para ello consulta el CAS local, la metadata guardada en SQLite y una caché en memoria llamada `ChunkIndex`.

La metadata del snapshot se guarda en SQLite. Cada snapshot contiene los archivos y carpetas recorridos, sus metadatos básicos, el estado del snapshot, el `origin_node_id` que lo creó y una referencia a la receta de chunks necesaria para reconstruir cada archivo.

Si existe un snapshot anterior completo de la misma raíz, los archivos que no han cambiado reutilizan directamente su receta anterior. Esto evita volver a leer y trocear archivos idénticos.

### Identidad del nodo origen

Cada snapshot se crea asociado a un `origin_node_id`.

Cuando el proceso puede resolver su identidad mediante membership, usa el `node_id` del miembro que coincide con `ADVERTISE_ADDR`. Si se ejecuta en modo local u offline, usa una identidad local persistida en `node_store/node_id.txt`.

El `origin_node_id` es importante porque la protección remota no debe contar el nodo que originó el snapshot como copia remota. Por eso replicación, verificación y restauración calculan el placement excluyendo ese nodo.

### Protección P2P

Después de crear un snapshot, los chunks quedan registrados en SQLite con estado de protección.

`stopan push` lee los chunks pendientes, obtiene una vista de nodos mediante membership y calcula los nodos destino usando HRW / rendezvous hashing. El replication factor indica cuántos nodos remotos deben proteger cada chunk.

Antes de enviar datos, el replicador pregunta a cada nodo qué hashes le faltan mediante `ProbeMissingChunks`. Después envía solo los chunks ausentes usando `ReplicateChunks`.

El protocolo de almacenamiento usa estados estructurados:

- `StoreStatus` para escritura remota;
- `RetrieveStatus` para lectura remota.

Cada nodo P2P mantiene su propio CAS, recibe chunks comprimidos por gRPC, valida su BLAKE3 antes de guardarlos y los devuelve cuando otro proceso los necesita.

### Verificación de protección remota

`stopan verify` audita la protección remota registrada en SQLite.

No descarga blobs completos. Recalcula el placement HRW vigente para cada chunk, excluye el `origin_node_id` y usa `ProbeMissingChunks` para comprobar si los nodos esperados siguen teniendo el chunk.

Si encuentra suficientes copias remotas, marca el chunk como `VERIFIED`. Si faltan copias, lo marca como `DEGRADED` y actualiza el error asociado.

Esto permite distinguir entre:

- chunks pendientes de protección;
- chunks colocados por `stopan push`;
- chunks verificados posteriormente;
- chunks cuya protección se ha degradado por pérdida de nodos, borrado de datos o cambios de cluster.

### Restauración

`stopan restore` reconstruye un snapshot a partir de la metadata guardada en SQLite.

Primero consulta los archivos y recetas del snapshot. Para cada chunk intenta leerlo desde el CAS local. Si no está disponible, puede consultar también el CAS local del nodo P2P y, si sigue faltando, obtiene la vista de nodos y lo solicita a los nodos que deberían tenerlo según HRW excluyendo el `origin_node_id` del snapshot.

La restauración remota usa `RetrieveChunkBatch`, por lo que puede pedir varios chunks a un mismo nodo en una sola llamada gRPC. Los chunks se resuelven por ventanas, pero se escriben siempre en el orden original del archivo.

La restauración se hace en una carpeta temporal `.incomplete`. Cada archivo se escribe primero como temporal y solo se mueve a su ruta final cuando se ha reconstruido completo.

Cuando todos los archivos se han restaurado correctamente, la carpeta incompleta se renombra como resultado final.

## Estructura

```text
.
├── Dockerfile
├── docker-compose.yml
├── README.md
├── requirements.txt
├── setup_fast_rabin.py
└── src/
    └── stopan/
        ├── __init__.py
        ├── __main__.py
        ├── backup/
        │   ├── config.py
        │   ├── identity.py
        │   ├── models.py
        │   ├── policy.py
        │   ├── service.py
        │   └── worker.py
        ├── cas/
        │   └── repository.py
        ├── chunking/
        │   ├── chunker.py
        │   ├── chunk_index.py
        │   ├── fast_rabin.c
        │   └── planner.py
        ├── cli/
        │   ├── backup.py
        │   ├── push.py
        │   ├── restore.py
        │   ├── root.py
        │   └── verify.py
        ├── metadata/
        │   └── database.py
        ├── node/
        │   ├── commit_engine.py
        │   ├── identity.py
        │   ├── membership.py
        │   ├── server.py
        │   └── storage_rpc.py
        ├── placement/
        │   ├── cluster_view.py
        │   └── hrw.py
        ├── protection/
        │   ├── policy.py
        │   ├── pusher.py
        │   ├── verifier.py
        │   ├── verify_config.py
        │   └── verify_models.py
        ├── protos/
        │   ├── membership.proto
        │   └── p2p_storage.proto
        ├── replication/
        │   ├── coordinator.py
        │   ├── outcomes.py
        │   ├── queue_iterator.py
        │   ├── rpc_pool.py
        │   └── streaming.py
        ├── restore/
        │   ├── cluster.py
        │   ├── config.py
        │   ├── fetch.py
        │   ├── paths.py
        │   ├── prefetcher.py
        │   ├── remote_client.py
        │   ├── restorer.py
        │   └── service.py
        └── scanning/
            └── scanner.py
```

## Requisitos locales

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

El archivo `requirements.txt` debe incluir al menos:

```text
grpcio
grpcio-tools
blake3
zstandard
```

## Compilar componentes generados

Regenerar los módulos Python de protobuf:

```bash
python -m grpc_tools.protoc   -I.   --python_out=.   --grpc_python_out=.   src/protos/p2p_storage.proto   src/protos/membership.proto
```

Compilar la extensión C:

```bash
python setup_fast_rabin.py build_ext --inplace
```

## Prueba local sin red

Crear una carpeta de prueba:

```bash
mkdir -p test_data/docs
printf "hola mundo\n" > test_data/archivo.txt
printf "contenido interno\n" > test_data/docs/info.txt
```

Crear un snapshot:

```bash
python -m stopan backup test_data
```

Indicar el número de hilos para procesar archivos:

```bash
python -m stopan backup test_data 4
```

Crear un snapshot con recorrido determinista:

```bash
python -m stopan backup test_data 4 --deterministic
```

Activar el fast-path local para reutilizar chunks ya presentes en el CAS:

```bash
python -m stopan backup test_data 4 --fast
```

Activar el fast-path remoto para saltar chunks que ya tienen evidencia suficiente de protección remota:

```bash
python -m stopan backup test_data 4 --fast-remote --seed localhost:50051 --rf 3
```

Forzar modo seguro, procesando todo sin saltos rápidos:

```bash
python -m stopan backup test_data 4 --safe
```

Restaurar el snapshot 1 en otra carpeta:

```bash
python -m stopan restore 1 restore_out
```

## Levantar nodos P2P con Docker

Construir y arrancar los nodos definidos en `docker-compose.yml`:

```bash
docker compose up --build
```

Para pararlos y borrar los volúmenes de prueba:

```bash
docker compose down -v
```

El `docker-compose.yml` define nodos con membership SWIM, repositorio CAS propio, identidad persistente por nodo y variables de rendimiento para replicación, verificación, restore y commit de almacenamiento.

## Sincronizar chunks a la red

Primero crea al menos un snapshot local:

```bash
python -m stopan backup test_data 4 --deterministic --rf 3
```

Con los nodos levantados, enviar los chunks pendientes usando replication factor 3:

```bash
python -m stopan push --seed localhost:50051 --rf 3
```

Opciones útiles:

```bash
python -m stopan push   --seed localhost:50051   --rf 3   --target-parallelism 4   --probe-batch-hashes 2048   --stream-inflight 64   --probe-timeout-s 10   --stream-timeout-s 60
```

Por defecto, `stopan push` aplica RF estricto: si no hay suficientes candidatos remotos para cumplir el RF deseado, no modifica `chunk_protection`.

Para permitir protección best-effort:

```bash
python -m stopan push --seed localhost:50051 --rf 3 --no-strict-rf
```

## Verificar protección remota

Después de hacer `push`, se puede auditar la protección remota:

```bash
python -m stopan verify --seed localhost:50051
```

Opciones útiles:

```bash
python -m stopan verify   --seed localhost:50051   --target-parallelism 4   --probe-batch-hashes 2048   --probe-timeout-s 10
```

Para volver a comprobar también chunks ya marcados como `VERIFIED`:

```bash
python -m stopan verify --seed localhost:50051 --reverify-verified
```

El verifier actualiza `chunk_protection`:

- `VERIFIED` si el chunk tiene suficientes copias remotas comprobadas;
- `DEGRADED` si faltan copias o hay errores al comprobar targets.

## Restaurar usando caché local o red

Si el bloque existe en el repositorio local, `restore` lo usa directamente. Si falta, intenta recuperarlo desde los nodos P2P que le corresponden.

```bash
python -m stopan restore 1 restored --seed localhost:50051 --rf 3
```

Opciones útiles:

```bash
python -m stopan restore 1 restored   --seed localhost:50051   --rf 3   --batch-target-parallelism 4   --prefetch-window 32
```

Para probar la recuperación desde red, una vez hecho `push`, se puede apartar o borrar el repositorio local `_data_chunks` y ejecutar `restore` de nuevo.
