## Flujo general

El sistema tiene tres operaciones principales:

1. crear snapshots locales de una carpeta;
2. proteger los chunks pendientes en una red P2P;
3. restaurar un snapshot usando la caché local o la red.

### Backup local

El backup empieza recorriendo la carpeta origen con `TreeWalker`.

Por cada archivo encontrado, `FileChunker` lo divide en chunks de tamaño variable usando Content-Defined Chunking. La extensión nativa `core.fast_rabin` calcula las fronteras de chunk de forma eficiente y cada chunk se identifica por su hash BLAKE3.

Antes de escribir un chunk, `ChunkPlanner` decide si hace falta procesarlo o si se puede reutilizar. Para eso consulta el repositorio local, la metadata guardada en SQLite y una caché en memoria llamada `ChunkIndex`.

La metadata del snapshot se guarda en SQLite. Cada snapshot contiene los archivos y carpetas recorridos, sus metadatos básicos, el estado del snapshot y una referencia a la receta de chunks necesaria para reconstruir cada archivo.

Si existe un snapshot anterior completo de la misma raíz, los archivos que no han cambiado pueden reutilizar directamente su receta anterior. Esto evita volver a leer y trocear archivos idénticos.

### Protección P2P

Después de crear un snapshot, los chunks quedan registrados en SQLite con estado de protección.

`replicator.py` lee los chunks pendientes, obtiene una vista de nodos mediante membership y calcula los nodos destino usando HRW / rendezvous hashing. El replication factor indica cuántos nodos deben proteger cada chunk.

Antes de enviar datos, el replicador pregunta a cada nodo qué hashes le faltan mediante `ProbeMissingChunks`. Después envía solamente los chunks ausentes usando `ReplicateChunks`.

El protocolo de almacenamiento usa estados estructurados:

- `StoreStatus` para escritura remota;
- `RetrieveStatus` para lectura remota.

`store_service.py` mantiene un repositorio CAS propio, recibe chunks comprimidos por gRPC, valida su BLAKE3 antes de guardarlos y los devuelve cuando otro proceso los necesita.

### Restauración

`restore.py` reconstruye un snapshot a partir de la metadata guardada en SQLite.

Primero consulta los archivos y recetas del snapshot. Para cada chunk intenta leerlo desde el repositorio local. Si no está disponible, obtiene la vista de nodos y lo solicita a los nodos que deberían tenerlo según el mismo algoritmo de placement.

La restauración remota usa `RetrieveChunkBatch`, por lo que puede pedir varios chunks a un mismo nodo en una sola llamada gRPC. Los chunks se resuelven por ventanas, pero se escriben siempre en el orden original del archivo.

La restauración se hace en una carpeta temporal `.incomplete`. Cada archivo se escribe primero como temporal y solo se mueve a su ruta final cuando se ha reconstruido completo.

Cuando todos los archivos se han restaurado correctamente, la carpeta incompleta se renombra como resultado final.

## Estructura

```text
.
├── main.py                 # crea snapshots locales de una carpeta
├── restore.py              # restaura snapshots desde caché local o nodos P2P
├── replicator.py           # protege chunks pendientes en la red P2P
├── store_service.py        # servidor gRPC de almacenamiento y membership
├── setup.py                # compila core.fast_rabin y regenera protos
├── Dockerfile
├── docker-compose.yml
├── core/
│   ├── __init__.py
│   ├── chunk_index.py
│   ├── chunker.py
│   ├── cluster_view.py
│   ├── database.py
│   ├── fast_rabin.c
│   ├── placement.py
│   ├── planner.py
│   ├── protection.py
│   ├── replication.py
│   ├── repository.py
│   └── scanner.py
└── protos/
    ├── __init__.py
    ├── membership.proto
    └── p2p_storage.proto
```

## Requisitos locales

```bash
python -m venv venv
source venv/bin/activate
pip install grpcio grpcio-tools blake3 zstandard
```

## Compilar componentes generados

Regenerar los módulos Python de protobuf:

```bash
python setup.py build_protos
```

Compilar la extensión C:

```bash
python setup.py build_ext --inplace
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
python main.py backup test_data
```

Se puede indicar el número de hilos para procesar archivos:

```bash
python main.py backup test_data 4
```

Crear un snapshot con recorrido determinista:

```bash
python main.py backup test_data 4 --deterministic
```

Activar el fast-path local para reutilizar chunks ya presentes en el CAS:

```bash
python main.py backup test_data 4 --fast
```

Activar el fast-path remoto para saltar chunks que ya tienen evidencia suficiente de protección remota:

```bash
python main.py backup test_data 4 --fast --fast-remote --membership-seed localhost:50051 --rf 3
```

Forzar modo seguro, procesando todo sin saltos rápidos:

```bash
python main.py backup test_data 4 --safe
```

Restaurar el snapshot 1 en otra carpeta:

```bash
python restore.py 1 restore_out
```

## Levantar nodos P2P con Docker

Construir y arrancar los cuatro nodos definidos en `docker-compose.yml`:

```bash
docker compose up --build
```

Para pararlos y borrar los volúmenes de prueba:

```bash
docker compose down -v
```

El `docker-compose.yml` define cuatro nodos con membership SWIM, repositorio CAS propio y variables de rendimiento para replicación, restore y commit de almacenamiento.

## Sincronizar chunks a la red

Primero crea al menos un snapshot local:

```bash
python main.py backup test_data 4 --deterministic --rf 3
```

Con los nodos levantados, enviar los chunks pendientes usando replication factor 3:

```bash
python replicator.py push --seed localhost:50051 --rf 3
```

Opciones útiles:

```bash
python replicator.py push \
  --seed localhost:50051 \
  --rf 3 \
  --target-parallelism 4 \
  --probe-batch-hashes 2048 \
  --stream-inflight 64 \
  --probe-timeout-s 10 \
  --stream-timeout-s 60
```

El estado de protección se guarda en SQLite, así que si el proceso se corta, se puede reanudar volviendo a ejecutar el comando.

## Restaurar usando caché local o red

Si el bloque existe en el repositorio local, `restore.py` lo usa directamente. Si falta, intenta recuperarlo desde los nodos P2P que le corresponden.

```bash
python restore.py 1 restored --seed localhost:50051 --rf 3
```

Opciones útiles:

```bash
python restore.py 1 restored \
  --seed localhost:50051 \
  --rf 3 \
  --batch-target-parallelism 4 \
  --prefetch-window 32
```

Para probar la recuperación desde red, una vez hecho `push`, se puede apartar o borrar el repositorio local `_data_chunks` y ejecutar `restore.py` de nuevo.

