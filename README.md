# Stopan

Stopan es un sistema de respaldo distribuido basado en direccionamiento por contenido. Cada backup genera snapshots locales, divide los datos en chunks identificados por BLAKE3 y registra la metadata necesaria para reconstruirlos en una base SQLite.

La protección remota se realiza después del backup mediante `push`. Stopan puede distribuir chunks completos por replicación o generar shards mediante erasure coding. El ciclo de vida se completa con verificación remota, restauración y limpieza controlada de artefactos locales.

La metadata se protege de forma independiente mediante object graphs cifrados y metadata packs firmados. Esto permite recuperar la base de metadata incluso si se pierde el nodo que creó los snapshots.

## Capacidades principales

- Snapshots de directorios locales.
- División de datos mediante Content-Defined Chunking.
- Identificación de chunks con BLAKE3.
- CAS local comprimido con Zstandard.
- Metadata persistida en SQLite.
- Nodo de almacenamiento basado en gRPC.
- Membership entre nodos con un protocolo tipo SWIM.
- Placement remoto mediante HRW/Rendezvous Hashing.
- Protección remota por replicación completa de chunks.
- Protección remota mediante erasure coding sobre data packs.
- Verificación remota sin descarga de blobs completos.
- Restore local, desde réplicas remotas, desde EC o en modo combinado.
- Protección de metadata mediante metadata packs cifrados y firmados.
- Limpieza local mediante GC.
- Empaquetado `.deb`, servicios systemd y jobs periódicos.
- Clúster Docker de demostración con cuatro nodos.

## Instalación

Stopan se distribuye como paquete `.deb`. Si ya se dispone del paquete generado, puede instalarse con:

```bash
sudo apt install ./stopan_1.0.0_amd64.deb
```

El paquete instala el comando `stopan`, las unidades systemd, los scripts de jobs y una configuración base.

La configuración principal queda en:

```text
/etc/stopan/stopan.yaml
```

La referencia completa del YAML está en [`docs/config-reference.md`](docs/config-reference.md).

## Construcción del paquete `.deb`

Clonando el repositorio se puede construir el paquete localmente. 
En una máquina Debian o Ubuntu, primero deben instalarse las dependencias de construcción:

```bash
sudo apt update
sudo apt install -y \
  build-essential \
  debhelper \
  devscripts \
  python3-dev \
  python3-pip \
  python3-setuptools \
  python3-venv \
  python3-wheel
```

Desde la raíz del repositorio:

```bash
dpkg-buildpackage -us -uc -b
```

El paquete se genera en el directorio padre del repositorio, con un nombre similar a:

```text
../stopan_1.0.0_amd64.deb
```

Los detalles de desarrollo, protobuf, extensión nativa y empaquetado están en [`docs/development.md`](docs/development.md).

## Configuración inicial

Cada máquina que participe en el clúster debe inicializar su configuración de nodo:

```bash
sudo stopan init node \
  --advertise-addr 192.168.1.10:50051 \
  --bind-addr '[::]:50051'
```

En un clúster con varios nodos, se pueden declarar seeds durante la inicialización. Si no están disponibles justo al arrancar, el nodo mantiene el servicio y reintenta la incorporación con una espera creciente y acotada hasta descubrir un par:

```bash
sudo stopan init node \
  --advertise-addr 192.168.1.10:50051 \
  --bind-addr '[::]:50051' \
  --seed 192.168.1.11:50051 \
  --seed 192.168.1.12:50051
```

Si el nodo va a crear o recuperar metadata packs, también debe inicializarse la identidad de metadata:

```bash
sudo stopan init metadata
```

El archivo `metadata_identity.json` y la passphrase asociada deben conservarse de forma segura. Sin esos datos no es posible validar ni recuperar metadata packs cifrados ante una pérdida del nodo origen.

## Uso básico

Crear un snapshot local:

```bash
stopan backup /ruta/a/datos
```

Consultar los snapshots registrados en el catálogo local:

```bash
stopan snapshot list
stopan snapshot show <SNAPSHOT_ID_O_UUID>
```

`backup` no envía datos a otros nodos. Para proteger los datos fuera del nodo origen hay que ejecutar `push`:

```bash
stopan push
```

Verificar la protección remota:

```bash
stopan verify
```

Restaurar un snapshot:

```bash
stopan restore <SNAPSHOT_ID>
```

Inspeccionar candidatos a limpieza local:

```bash
stopan gc all
```

La referencia completa del CLI está en [`docs/cli-reference.md`](docs/cli-reference.md).

## Servicio de nodo

El nodo persistente se arranca con systemd:

```bash
sudo systemctl enable --now stopan-node.service
```

También puede ejecutarse directamente desde el CLI:

```bash
stopan node
```

Para consultar su estado:

```bash
stopan node status
```

La parada local puede solicitarse desde el propio CLI:

```bash
sudo stopan node stop
```

La parada cierra primero la admisión de trabajo nuevo, anuncia el estado `LEFT` a los pares conocidos y contactos de bootstrap y espera las operaciones CLI y RPC ya iniciadas antes de terminar el proceso. `systemctl stop stopan-node.service` utiliza la misma ruta mediante `SIGTERM`.

El servicio de nodo expone los RPC usados para membership, almacenamiento remoto, shards EC y metadata packs.

## Protección de metadata

Un flujo básico de protección de metadata consiste en exportar la base local a un object graph cifrado, crear un metadata pack y distribuirlo a otros nodos:

```bash
stopan metadata graph export --pack
stopan metadata pack push
```

La recuperación de metadata está documentada en [`docs/metadata-recovery.md`](docs/metadata-recovery.md). La recuperación completa de una máquina perdida está documentada en [`docs/disaster-recovery.md`](docs/disaster-recovery.md).

## Clúster Docker de demostración

El repositorio incluye un entorno Docker Compose con cuatro nodos:

```bash
docker compose up --build
```

## Documentación

La documentación detallada está en `docs/`:

- [`docs/architecture.md`](docs/architecture.md): arquitectura interna del sistema.
- [`docs/cli-reference.md`](docs/cli-reference.md): comandos, opciones, efectos y validaciones del CLI.
- [`docs/config-reference.md`](docs/config-reference.md): configuración de `stopan.yaml`.
- [`docs/local-network-deployment.md`](docs/local-network-deployment.md): despliegue en una red local.
- [`docs/jobs.md`](docs/jobs.md): perfiles YAML y ejecución periódica con systemd.
- [`docs/metadata-recovery.md`](docs/metadata-recovery.md): protección y recuperación de metadata.
- [`docs/disaster-recovery.md`](docs/disaster-recovery.md): recuperación completa ante pérdida de una máquina.
- [`docs/development.md`](docs/development.md): entorno de desarrollo, protobuf, extensión nativa y empaquetado.
