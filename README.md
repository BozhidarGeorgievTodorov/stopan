## Flujo general

El sistema tiene tres operaciones principales:

1. crear snapshots locales de una carpeta;
2. sincronizar chunks pendientes con nodos P2P;
3. restaurar un snapshot usando la caché local o la red.

### Backup local

El backup empieza recorriendo la carpeta origen con `TreeWalker`.

Por cada archivo encontrado, `FileChunker` lo divide en chunks de tamaño variable usando Content-Defined Chunking. Cada chunk se identifica por su hash de contenido y se comprime antes de guardarse en el repositorio CAS.

Antes de escribir un chunk, `ChunkPlanner` decide si hace falta procesarlo o si se puede reutilizar. Para eso consulta el repositorio local, la metadata guardada en SQLite y una caché en memoria llamada `ChunkIndex`.

La metadata del snapshot se guarda en SQLite. Cada snapshot contiene los archivos y carpetas recorridos, sus metadatos básicos y una referencia a la receta de chunks necesaria para reconstruir cada archivo.

Durante el backup intervienen principalmente:

```text
TreeWalker
FileChunker
ChunkPlanner
ChunkIndex
CASRepository
MetadataDB
```

### Sincronización P2P

Después de crear un snapshot, los chunks nuevos quedan registrados en SQLite como pendientes de sincronización.

`sync_cli.py` lee esos chunks pendientes, calcula a qué nodo corresponde cada uno mediante hashing consistente y los envía por gRPC ya comprimidos.

Los nodos no crean snapshots ni recorren carpetas. Solo reciben, guardan y sirven chunks.

El estado de sincronización se guarda en SQLite, así que si el proceso se corta se puede volver a ejecutar el `push` y continuar con los chunks que falten.

### Restauración

`restore.py` reconstruye un snapshot a partir de la metadata guardada en SQLite.

Primero consulta los archivos y recetas del snapshot. Para cada chunk intenta leerlo desde el repositorio local. Si no está disponible, calcula qué nodo debería tenerlo y lo solicita por gRPC.

La restauración se hace en una carpeta temporal `.incomplete`. Cada archivo se escribe primero como temporal y solo se mueve a su ruta final cuando se ha reconstruido completo.

Cuando todos los archivos se han restaurado correctamente, la carpeta incompleta se renombra como resultado final.

## Estructura

```text
.
├── main.py                 # crea snapshots locales de una carpeta
├── restore.py              # restaura snapshots desde caché local o nodos P2P
├── sync_cli.py             # sincroniza chunks pendientes con la red P2P
├── node_server.py          # servidor gRPC de un nodo de almacenamiento
├── setup.py                # compila core.fast_rabin y regenera protos
├── Dockerfile
├── docker-compose.yml
├── core/
│   ├── __init__.py
│   ├── chunker.py
│   ├── chunk_index.py
│   ├── database.py
│   ├── fast_rabin.c
│   ├── planner.py
│   ├── repository.py
│   └── scanner.py
└── protos/
    ├── __init__.py
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

Crear un snapshot usando varios hilos:

```bash
python main.py backup test_data 4
```

Crear un snapshot con recorrido determinista:

```bash
python main.py backup test_data 4 --deterministic
```

Activar el fast-path para reutilizar chunks ya conocidos:

```bash
python main.py backup test_data 4 --fast
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

Para pararlos:

```bash
docker compose down -v
```

## Sincronizar chunks a la red

Primero crea al menos un snapshot local:

```bash
python main.py backup test_data
```

Con los nodos levantados, enviar los chunks pendientes:

```bash
python sync_cli.py push
```

El estado de sincronización se guarda en SQLite, así que si el proceso se corta, se puede reanudar volviendo a ejecutar el comando.

## Restaurar usando caché local o red

Si el bloque existe en el repositorio local, `restore.py` lo usa directamente. Si falta, intenta recuperarlo desde el nodo P2P que le corresponde.

```bash
python restore.py 1 restored
```

Para probar la recuperación desde red, una vez hecho `push`, borramos el repositorio local `_data_chunks`.
