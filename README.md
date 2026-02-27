El flujo principal es:

```text
TreeWalker -> FileChunker -> CASRepository
                       └──> MetadataDB
```

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
│   ├── database.py
│   ├── fast_rabin.c
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

También se puede indicar el número de hilos para procesar archivos:

```bash
python main.py backup test_data 4
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
