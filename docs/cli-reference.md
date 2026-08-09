# Referencia CLI de Stopan

Este documento describe el contrato público del comando `stopan`. Explica qué comandos existen, qué estado modifican, qué operaciones usan red y qué combinaciones de opciones rechaza el propio CLI.

Todos los parsers usan `allow_abbrev=False`. Los flags largos deben escribirse completos para evitar ambigüedades entre opciones parecidas.

La forma de invocación instalada es:

```bash
stopan <command> ...
```

En desarrollo se puede usar la forma equivalente:

```bash
python -m stopan <command> ...
```

La ayuda general se consulta con:

```bash
stopan --help
stopan help
stopan help backup
stopan backup --help
```


## Vista rápida de comandos

| Comando | Modifica estado | Usa red | Propósito | Nota principal |
|---|---:|---:|---|---|
| `backup` | Sí | Opcional | Crea un snapshot local | No replica datos. |
| `push` | Sí | Sí | Protege datos en nodos remotos | Usa `replication` o `ec`. |
| `verify` | Sí | Sí | Audita protección remota | Puede marcar estado degradado. |
| `restore` | Sí | Opcional | Reconstruye un snapshot | Usa red solo si `--remote-recovery` no es `none`. |
| `node` | Sí | Sí | Arranca el servidor gRPC | Requiere `node.advertise_addr`. |
| `node status` | No | Opcional | Diagnostica el nodo | Puede consultar una dirección explícita. |
| `init node` | Sí | No | Inicializa configuración de nodo | Requiere `--advertise-addr`. |
| `init metadata` | Sí | No | Inicializa identidad de metadata | Necesario para crear o descifrar packs. |
| `config example` | Opcional | No | Genera configuración de ejemplo | Solo escribe si se pasa `--out`. |
| `config validate` | No | No | Valida un YAML | No arranca servicios. |
| `metadata graph export` | Sí | No | Exporta SQLite a object graph | Puede crear un pack con `--pack`. |
| `metadata graph import` | Sí | No | Reconstruye SQLite desde object graph | Puede importar protección o crear filas `PENDING`. |
| `metadata pack push` | Sí | Sí | Distribuye metadata packs | Usa política separada de la de chunks. |
| `metadata pack recover` | Sí | Sí | Recupera metadata desde packs remotos | Puede descargar, importar y reconstruir DB. |
| `gc TARGET --dry-run` | No | No | Simula limpieza local | No borra artefactos. |
| `gc TARGET --apply` | Sí | No | Aplica limpieza local | Borra artefactos locales del target. |

## Compatibilidad de modos

| Comando | Modo | Acepta | Rechaza o limita | Nota |
|---|---|---|---|---|
| `push` | `replication` | `--remote-copies`, `--strict-remote-copies`, `--probe-batch-hashes`, `--stream-inflight` | Opciones exclusivas de EC | Replica chunks completos. |
| `push` | `ec` | `--ec-k`, `--ec-m`, `--ec-pack-size-bytes` | Opciones exclusivas de replicación | Distribuye shards de data packs. |
| `verify` | `replication` | `--probe-batch-hashes` | `--pack-hash` | Consulta presencia de chunks. |
| `verify` | `ec` | `--pack-hash` | `--probe-batch-hashes` | Consulta presencia de shards. |
| `restore` | `none` | `--out`, `--prefetch-window` | `--membership-seed`, `--replication-targets` | No usa red. |
| `restore` | `replication` o `auto` | `--replication-targets`, `--membership-seed` | Opciones de `push` EC | Consulta chunks remotos. |
| `restore` | `ec` | `--membership-seed` | `--replication-targets` | Reconstruye desde shards EC. |
| `metadata pack push` | pack distribuido | `--pack-copies`, `--strict-pack-copies` | Mezclas inválidas de `--pack-in`, `--object-store`, `--pack-out` y `--pack-dir` | La política de packs no es la política de chunks. |
| `metadata pack recover` | recuperación | `--target-hash`, `--vault-id`, `--download-only`, `--no-import-db` | Combinaciones incompatibles de descarga e importación | Elige un pack remoto válido y lo valida antes de importarlo. |

## Configuración común

La mayoría de comandos operativos aceptan:

```bash
--config CONFIG
```

Si no se indica, usan `/etc/stopan/stopan.yaml`. Cuando ese fichero no existe, los comandos que necesitan configuración fallan con un error controlado y recomiendan inicializarla con `stopan init node`.

Los valores persistentes se definen en `stopan.yaml`. Los flags del CLI permiten modificar una ejecución concreta.

La precedencia habitual es:

```text
flag CLI
configuración YAML
default del modelo de configuración
```


## Opciones compartidas

Algunas opciones aparecen en varios comandos porque afectan a configuración, membership, límites de red o cifrado.

`--membership-seed HOST:PORT` indica un punto de entrada al clúster para obtener una vista de membership. No fuerza que toda la operación se ejecute contra ese nodo.

`--target-parallelism N` controla cuántos targets remotos se procesan en paralelo.

`--probe-batch-hashes N` controla cuántos hashes se agrupan por consulta de inventario remoto.

`--probe-timeout-s SECONDS` limita el tiempo de espera de consultas de presencia.

`--stream-timeout-s SECONDS` limita el tiempo de espera de operaciones de streaming.

`--stream-inflight N` limita cuántos elementos pueden quedar pendientes dentro de un stream.

`--max-message-bytes BYTES` fija el tamaño máximo de mensaje gRPC usado por la operación.

`--commit-every N` controla cada cuántos resultados se persiste el progreso.

`--rpc-timeout-s SECONDS` limita el tiempo de espera en operaciones RPC de metadata packs.

Los comandos que cifran o descifran metadata pueden aceptar:

```bash
--scrypt-n N
--scrypt-r N
--scrypt-p N
--metadata-key-length N
```

`--scrypt-n` debe ser al menos 2 y potencia de dos. `--scrypt-r` y `--scrypt-p` deben ser al menos 1. `--metadata-key-length` debe ser al menos 32.

## Códigos de salida y errores

Los errores controlados se muestran sin traceback salvo que `STOPAN_DEBUG` esté activado. Los prefijos son estables:

```text
Error de uso de Stopan             flags inválidos o combinación CLI/config incompatible
Error de configuración de Stopan   YAML ausente, inválido o insuficiente
Error de datos de Stopan           metadata, hashes, packs o estado persistido inconsistente
Error de almacenamiento de Stopan  rutas o stores locales no accesibles
Error de red de Stopan             membership, peers, RPC o streams fallidos
Error de dependencias de Stopan    dependencia runtime no disponible
Error de Stopan                    fallo no clasificado
```

Los códigos relevantes son:

```text
0    operación completada correctamente
1    error general controlado o fallo operativo
2    uso/configuración inválidos, o protección incompleta en comandos que pueden devolver degradación
130  interrupción por teclado o ejecución interrumpida con progreso persistido
```

Algunos comandos devuelven `2` sin que exista un fallo interno del programa. Por ejemplo, `push` puede devolver `2` si no alcanza la protección remota pedida, `verify` si detecta degradación y `restore` si no logra completar la reconstrucción.

## Exportación automática de metadata

`backup`, `push` y `verify` aceptan opciones para exportar metadata object graph y crear metadata packs después de modificar metadata.

Flags compartidos:

```bash
--metadata-object-graph
--no-metadata-object-graph
--metadata-object-store DIR
--metadata-passphrase-file FILE
--metadata-identity-file FILE
--metadata-object-pack
--no-metadata-object-pack
--metadata-object-pack-dir DIR
```

El flujo completo de protección y recuperación de metadata está documentado en [`docs/metadata-recovery.md`](metadata-recovery.md).

## `stopan backup`

Uso:

```bash
stopan backup [opciones] SOURCE_PATH [WORKERS]
```

Argumentos:

```text
SOURCE_PATH  carpeta origen a respaldar
WORKERS      número de hilos. Si se omite, usa backup.workers
```

Opciones:

```bash
--fast
--fast-remote
--safe
--deterministic
--desired-remote-copies N
--membership-seed HOST:PORT
```

`--fast` activa el fast-path local para saltar chunks que ya existen en el CAS local.

`--fast-remote` permite saltar chunks que ya tienen evidencia suficiente de protección remota según la metadata y el placement actual. No replica datos. Si se pasa `--fast-remote` sin `--fast`, el comando activa `--fast` implícitamente.

`--safe` desactiva tanto el fast-path local como el remoto y fuerza el procesamiento completo. Tiene prioridad sobre `--fast` y `--fast-remote`.

`--deterministic` usa un recorrido determinista del árbol de ficheros. Es útil para pruebas y comparaciones reproducibles.

`--desired-remote-copies` no replica nada. Solo registra en metadata cuántas copias remotas se desean para que `push` y `verify` conozcan el objetivo.

`--membership-seed` se usa para consultar membership cuando se necesita validar el epoch asociado al fast-path remoto.

Ejemplos:

```bash
stopan backup /ruta/a/datos
stopan backup /ruta/a/datos --deterministic
```

Validaciones relevantes:

```text
WORKERS debe ser al menos 1.
SOURCE_PATH debe existir y ser un directorio.
--desired-remote-copies puede ser 0 o mayor.
```

## `stopan push`

Uso:

```bash
stopan push [opciones]
```

`push` protege datos en nodos remotos. Puede trabajar en modo `replication` o en modo `ec`.

En modo `replication`, envía chunks completos a nodos remotos. En modo `ec`, agrupa chunks en data packs, genera shards de erasure coding y distribuye esos shards entre nodos remotos.

Opciones comunes:

```bash
--membership-seed HOST:PORT
--protection-mode replication|ec
--limit N
--scope pending|snapshot|all-reachable|all-known-chunks
--snapshot-id ID
--stream-timeout-s SECONDS
--max-message-bytes BYTES
--commit-every N
```

El scope por defecto es `pending`.

`pending` procesa elementos que están pendientes, degradados, fallidos o asociados a un epoch obsoleto según metadata.

`snapshot` limita el trabajo a chunks alcanzables desde un snapshot concreto. Puede seleccionarse con `--scope snapshot --snapshot-id ID`. Pasar solo `--snapshot-id ID` también fuerza scope `snapshot`.

`all-reachable` procesa chunks alcanzables desde snapshots completos.

`all-known-chunks` procesa todos los chunks conocidos por la metadata.

`--limit N` acota el trabajo de una ejecución. En replicación limita chunks candidatos. En EC limita packs pendientes de reintento y chunks nuevos a empaquetar. No es un límite de bytes y no modifica la política de protección.

`--stream-timeout-s`, `--max-message-bytes` y `--commit-every` son ajustes avanzados para controlar RPCs largos, tamaño máximo de mensajes y persistencia de progreso.

Ejemplos:

```bash
stopan push
stopan push --limit 100
stopan push --snapshot-id 12
```

Validaciones comunes:

```text
--limit debe ser al menos 1.
--snapshot-id debe ser al menos 1.
--snapshot-id no se puede combinar con scopes distintos de pending o snapshot.
--scope snapshot requiere --snapshot-id.
--stream-timeout-s debe ser mayor que 0.
--max-message-bytes debe ser al menos 1.
--commit-every debe ser al menos 1.
```

### `push` en modo replication

Opciones específicas:

```bash
--remote-copies N
--target-parallelism N
--probe-batch-hashes N
--stream-inflight N
--probe-timeout-s SECONDS
--strict-remote-copies
--no-strict-remote-copies
```

`--remote-copies` indica cuántas copias completas remotas se requieren por chunk. La copia local del nodo origen no cuenta.

`--strict-remote-copies` exige suficientes nodos remotos elegibles antes de empezar. Si no hay capacidad suficiente, aborta con código `2` sin marcar chunks como protegidos.

`--no-strict-remote-copies` permite una ejecución best-effort. El comando intenta avanzar con los nodos disponibles, pero puede terminar con protección incompleta.

`--target-parallelism`, `--probe-batch-hashes`, `--stream-inflight` y `--probe-timeout-s` ajustan concurrencia, tamaño de probes, ventana de streaming y tiempo máximo de espera al consultar inventario remoto.

Ejemplos:

```bash
stopan push --protection-mode replication
stopan push --remote-copies 3 --strict-remote-copies
```

Validaciones específicas:

```text
--remote-copies debe ser al menos 1 en modo replication.
--target-parallelism debe ser al menos 1.
--probe-batch-hashes debe ser al menos 1.
--stream-inflight debe ser al menos 1.
--probe-timeout-s debe ser mayor que 0.
Las opciones de EC se rechazan en modo replication.
```

### `push` en modo EC

Opciones específicas:

```bash
--ec-k N
--ec-m N
--ec-pack-size-bytes BYTES
```

`--ec-k` es el número de data shards por data pack.

`--ec-m` es el número de parity shards.

Stopan necesita colocar `ec_k + ec_m` shards en nodos remotos elegibles distintos. El nodo origen se excluye del placement.

`--ec-m 0` está permitido, pero no aporta redundancia. En ese caso se necesitan todos los shards para reconstruir el pack.

`--ec-pack-size-bytes` controla el tamaño objetivo de los data packs que se convierten en shards EC. Es un ajuste de rendimiento y granularidad, no un factor de redundancia.

Ejemplo:

```bash
stopan push --protection-mode ec --ec-k 2 --ec-m 1
```

Validaciones específicas:

```text
--ec-k debe ser al menos 1.
--ec-m puede ser 0 o mayor.
--ec-pack-size-bytes debe ser al menos 1.
Las opciones exclusivas de replication se rechazan en modo EC.
```

`push replication` devuelve `0` si no hay fallos ni degradación y `2` si queda protección incompleta o si el modo estricto impide empezar.

`push ec` devuelve `0` si no hay packs fallidos ni degradados y `2` si hay packs degradados, packs fallidos o targets remotos insuficientes.

## `stopan verify`

Uso:

```bash
stopan verify [opciones]
```

`verify` audita presencia remota sin descargar blobs completos. En modo `replication`, consulta inventario remoto con `ProbeMissingChunks`. En modo `ec`, verifica presencia de shards de data packs.

Opciones comunes:

```bash
--protection-mode replication|ec
--membership-seed HOST:PORT
--probe-timeout-s SECONDS
--target-parallelism N
--limit N
--scope pending|snapshot|all-reachable|all-known-chunks
--snapshot-id ID
--reverify-verified
--max-message-bytes BYTES
```

El scope tiene la misma semántica que en `push`.

`--reverify-verified` incluye elementos ya verificados para auditarlos otra vez.

`--target-parallelism`, `--probe-timeout-s`, `--probe-batch-hashes` y `--max-message-bytes` permiten ajustar la auditoría remota cuando el clúster, la red o el tamaño de los lotes lo requieren.

En modo replication también acepta:

```bash
--probe-batch-hashes N
```

En modo EC también acepta:

```bash
--pack-hash HASH
```

`--pack-hash` verifica un data pack concreto. Solo aplica a EC.

Ejemplos:

```bash
stopan verify
stopan verify --protection-mode ec
stopan verify --protection-mode ec --pack-hash 012345...
```

Validaciones relevantes:

```text
--limit debe ser al menos 1.
--snapshot-id debe ser al menos 1.
--target-parallelism debe ser al menos 1.
--probe-batch-hashes debe ser al menos 1.
--probe-timeout-s debe ser mayor que 0.
--max-message-bytes debe ser al menos 1.
--snapshot-id fuerza scope snapshot.
--scope snapshot requiere --snapshot-id.
--pack-hash solo aplica a EC.
--pack-hash no se puede combinar con --scope ni --snapshot-id.
--probe-batch-hashes limita el tamaño de los lotes remotos en replication y ec.
```

El comando devuelve `0` si no detecta degradación. En replication devuelve `2` si hay chunks degradados. En EC devuelve `2` si hay packs degradados o fallidos.

## `stopan restore`

Uso:

```bash
stopan restore [opciones] SNAPSHOT_ID
```

Argumento:

```text
SNAPSHOT_ID  identificador del snapshot a restaurar
```

`restore` reconstruye un snapshot en un directorio de salida. Por defecto no usa red.

Opciones:

```bash
--out DIR
--remote-recovery none|replication|ec|auto
--replication-targets N
--membership-seed HOST:PORT
--prefetch-window N
--batch-target-parallelism N
```

`--out` usa `restore_out` por defecto. El contenido restaurado se escribe bajo un subdirectorio `snapshot_<id>` dentro de esa ruta base.

`--remote-recovery none` es el default. Solo usa stores locales.

`--remote-recovery replication` consulta nodos remotos para recuperar chunks completos ausentes.

`--remote-recovery ec` reconstruye desde data packs EC registrados.

`--remote-recovery auto` intenta recuperación por replication y después EC para lo que siga faltando.

`--replication-targets` solo aplica a `replication` y `auto`. No es factor de protección. Solo limita cuántos targets HRW se consultan por chunk ausente.

`--prefetch-window` controla cuántos chunks se resuelven por ventana de trabajo.

`--batch-target-parallelism` controla cuántos targets remotos se consultan en paralelo durante la recuperación.

`--membership-seed` solo aplica cuando el modo usa red.

Ejemplos:

```bash
stopan restore 1
stopan restore 1 --out restored
stopan restore 1 --remote-recovery auto
```

Validaciones relevantes:

```text
SNAPSHOT_ID debe ser al menos 1.
--replication-targets solo aplica a replication o auto.
--replication-targets debe ser al menos 1 cuando se pasa.
--membership-seed se rechaza con --remote-recovery none.
--prefetch-window debe ser al menos 1.
--batch-target-parallelism debe ser al menos 1.
```

Si el restore se interrumpe, devuelve `130`. Si no completa la reconstrucción, devuelve `2`.

## `stopan node`

Uso como servicio:

```bash
stopan node [opciones]
```

Sin subcomando, `node` arranca el servidor gRPC configurado. Requiere `node.advertise_addr`. El servidor expone almacenamiento P2P, shards EC, membership y metadata pack storage sobre el mismo proceso gRPC.


Subcomando de diagnóstico:

```bash
stopan node status [opciones]
```

`node status` acepta:

```bash
--address HOST:PORT
```

`node status` comprueba configuración local, identidad del nodo, rutas locales y RPCs de membership/storage. Si se pasa `--address`, consulta esa dirección como destino remoto. Si no se pasa, usa `node.advertise_addr` o el primer seed configurado.

Ejemplos:

```bash
stopan node
stopan node status
stopan node status --address 192.168.1.20:50051
```

## `stopan init`

Uso:

```bash
stopan init node [opciones]
stopan init metadata [opciones]
```

### `stopan init node`

Inicializa o actualiza la configuración principal de nodo y prepara directorios.

Opciones:

```bash
--config PATH
--advertise-addr HOST:PORT
--bind-addr ADDR
--token TOKEN
--seed HOST:PORT
--identity-file PATH
--catalog-file PATH
--local-chunk-dir DIR
--custody-dir DIR
--remote-copies N
--strict-remote-copies
--no-strict-remote-copies
```

`--advertise-addr` es obligatorio. Es la dirección pública o anunciada del nodo.

`--bind-addr` define dónde escucha localmente el servidor gRPC. Su default es `[::]:50051`.

`--seed` puede repetirse. Si no se pasa ningún seed, Stopan usa `advertise_addr` como seed inicial.

`--identity-file` y `--catalog-file` fijan el estado operativo del nodo. `--local-chunk-dir` configura el CAS propio y `--custody-dir` la raíz de contenido recibido. El comando crea los directorios necesarios.

`--remote-copies` actualiza `protection.remote_copies` y puede ser 0 o mayor.

`--strict-remote-copies` y `--no-strict-remote-copies` actualizan `protection.strict_remote_copies`.

Ejemplo:

```bash
stopan init node --advertise-addr HOST:PORT
```

En instalaciones de sistema puede requerir permisos de administrador para escribir en `/etc/stopan` o `/var/lib/stopan`.

### `stopan init metadata`

Uso:

```bash
stopan init metadata [opciones]
```

Inicializa passphrase, identidad criptográfica y `metadata.owner_id`.

Opciones:

```bash
--config PATH
```

Requiere que la configuración principal ya exista y que tenga configurados `metadata.passphrase_file` y `metadata.identity_file`. Si el fichero de passphrase no existe, la pide por terminal y la guarda. Si el identity file no existe, lo crea cifrado con esa passphrase. Finalmente actualiza `metadata.owner_id` en el YAML.

Este comando es imprescindible antes de crear o descifrar `.stopanmetapack` de forma operativa. El administrador debe conservar con seguridad tanto la passphrase como `metadata_identity.json`. El `owner_id` por sí solo no permite descifrar packs.

Ejemplo:

```bash
stopan init metadata
```

## `stopan config`

Uso:

```bash
stopan config example [opciones]
stopan config validate CONFIG_PATH
```

`example` imprime un `stopan.yaml` de ejemplo por stdout o lo escribe en `--out`.

`validate` carga y valida un YAML de configuración. No arranca servicios ni modifica datos.

Opciones de `example`:

```bash
--out PATH
```

Argumentos de `validate`:

```text
CONFIG_PATH  ruta del YAML a validar
```

Ejemplos:

```bash
stopan config example
stopan config example --out stopan.example.yaml
stopan config validate /etc/stopan/stopan.yaml
```

## `stopan metadata`

`metadata` agrupa comandos para consultar, exportar, empaquetar, distribuir, recuperar e importar metadata.

`metadata graph` gestiona el object graph cifrado. `metadata pack` gestiona `.stopanmetapack` locales y distribuidos.

Subcomandos principales:

```text
status          muestra estado efectivo del metadata vault
identity-show   muestra el owner efectivo e información de identidad
graph           gestiona el metadata object graph cifrado
pack            gestiona metadata packs locales y distribuidos
```

### `metadata status`

Uso:

```bash
stopan metadata status [opciones]
```

Muestra estado efectivo del metadata vault: DB local, object store, passphrase, identidad, owner, distributed pack store, política de copias de packs, límites de packs distribuidos y políticas de GC.

No modifica datos.

### `metadata identity-show`

Uso:

```bash
stopan metadata identity-show [opciones]
```

Opciones:

```bash
--owner-id OWNER
--identity-file PATH
```

Resuelve el owner efectivo. La prioridad es `--owner-id`, después `metadata.owner_id` y por último `metadata.identity_file`.

Si lee un identity file, muestra algoritmo, owner, claves públicas y si las claves privadas están cifradas.

## `stopan metadata graph`

Subcomandos:

```text
status   inspecciona el metadata object store cifrado
export   exporta SQLite local a object graph cifrado
import   reconstruye SQLite local desde el object graph cifrado
```

### `metadata graph status`

Uso:

```bash
stopan metadata graph status [opciones]
```

Opciones:

```bash
--object-store DIR
--decrypt-latest
--passphrase-file FILE
```

Sin `--decrypt-latest`, muestra cabecera del store y si existe latest. Con `--decrypt-latest`, descifra el latest pointer y muestra resumen del estado exportado.

### `metadata graph export`

Uso:

```bash
stopan metadata graph export [opciones]
```

Opciones:

```bash
--object-store DIR
--passphrase-file FILE
--identity-file FILE
--no-protection
--pack
--pack-out PATH
--pack-dir DIR
--scrypt-n N
--scrypt-r N
--scrypt-p N
--metadata-key-length N
```

Exporta el estado actual del catálogo SQLite a un metadata object graph incremental y cifrado. Por defecto incluye `chunk_protection`. Con `--no-protection` no la incluye.

Si se pasa `--pack`, además crea un `.stopanmetapack` transportable a partir del latest del object graph. En ese caso se usa `--identity-file` para cifrar el pack.

Validaciones relevantes:

```text
--pack-out requiere --pack.
--pack-dir requiere --pack.
--pack-out y --pack-dir son incompatibles.
```

### `metadata graph import`

Uso:

```bash
stopan metadata graph import [opciones]
```

Opciones:

```bash
--object-store DIR
--passphrase-file FILE
--no-protection
--default-desired-remote-copies N
```

Reconstruye un catálogo SQLite vacío desde el latest del metadata object store cifrado.

Con protección incluida, importa también `chunk_protection`. Con `--no-protection`, o cuando el graph no tiene índice de protección, crea filas `PENDING` para chunks conocidos usando `--default-desired-remote-copies` o `protection.remote_copies`.

`--default-desired-remote-copies` puede ser 0 o mayor.

## `stopan metadata pack`

Los subcomandos `metadata pack` trabajan con `.stopanmetapack`. Un pack es un artefacto cifrado y transportable derivado del latest del metadata object graph.

Subcomandos:

```text
create          crea un metadata pack local
inspect         inspecciona un pack local
list            lista packs locales generados
import          importa un pack a un object store local
push            distribuye un pack a nodos remotos
discover        descubre packs remotos por owner
verify          verifica presencia remota de packs
recover         recupera metadata desde packs remotos
local-store     guarda un pack en el store distribuido local
local-list      lista packs del store distribuido local
local-retrieve  extrae un pack del store distribuido local
```

### `metadata pack create`

Uso:

```bash
stopan metadata pack create [opciones]
```

Opciones:

```bash
--object-store DIR
--out PATH
--pack-dir DIR
--passphrase-file FILE
--identity-file FILE
--scrypt-n N
--scrypt-r N
--scrypt-p N
--metadata-key-length N
```

Crea un `.stopanmetapack` desde el latest del object store. Si no se pasa `--out`, escribe en `metadata.generated_pack_dir` o en `--pack-dir` si se especifica.

Validación: `--out` y `--pack-dir` son incompatibles.

### `metadata pack inspect`

Uso:

```bash
stopan metadata pack inspect [opciones] PATH
```

Argumento:

```text
PATH  ruta del fichero .stopanmetapack
```

Opciones:

```bash
--decrypt
--full-validation
--passphrase-file FILE
--identity-file FILE
```

Sin `--decrypt`, muestra la cabecera del pack. Con `--decrypt`, descifra el contenido y valida su resumen. `--full-validation` requiere `--decrypt` y verifica además el hash, el tipo y el tamaño de todos los objetos sin conservar en memoria la colección decodificada.

### `metadata pack list`

Uso:

```bash
stopan metadata pack list [opciones]
```

Opciones:

```bash
--pack-dir DIR
```

Lista packs locales sin descifrarlos. Si se pasa `--pack-dir`, usa ese directorio. En caso contrario usa `metadata.generated_pack_dir`.

### `metadata pack import`

Uso:

```bash
stopan metadata pack import [opciones] PATH
```

Argumento:

```text
PATH  ruta del fichero .stopanmetapack
```

Opciones:

```bash
--object-store DIR
--passphrase-file FILE
--identity-file FILE
--scrypt-n N
--scrypt-r N
--scrypt-p N
--metadata-key-length N
```

Importa un `.stopanmetapack` a un metadata object store local. Descifra y valida el pack con la identidad y passphrase configuradas o pasadas por CLI.

### `metadata pack push`

Uso:

```bash
stopan metadata pack push [opciones]
```

Opciones:

```bash
--pack-in PATH
--object-store DIR
--passphrase-file FILE
--pack-out PATH
--pack-dir DIR
--owner-id OWNER
--identity-file FILE
--membership-seed HOST:PORT
--pack-copies N
--strict-pack-copies
--no-strict-pack-copies
--target-parallelism N
--rpc-timeout-s SECONDS
--max-message-bytes BYTES
--scrypt-n N
--scrypt-r N
--scrypt-p N
--metadata-key-length N
```

Distribuye metadata packs a nodos remotos. Si se pasa `--pack-in`, distribuye ese pack existente. Si no se pasa, crea o reutiliza un pack local correspondiente al latest del object graph y lo distribuye.

`--pack-copies` controla cuántas copias remotas del metadata pack se buscan. Esta política es independiente de `protection.remote_copies`, que aplica a chunks de datos. `--pack-copies 0` es válido y significa no enviar el pack.

`--strict-pack-copies` exige suficientes targets remotos antes de distribuir. `--no-strict-pack-copies` fuerza modo no estricto para esa ejecución.

`--target-parallelism`, `--rpc-timeout-s` y `--max-message-bytes` ajustan la concurrencia, el tiempo máximo de espera y el límite de cada mensaje del flujo. El tamaño completo del pack queda limitado por `metadata.max_distributed_pack_bytes`.

Validaciones relevantes:

```text
--pack-in es incompatible con --object-store.
--pack-in es incompatible con --pack-out.
--pack-in es incompatible con --pack-dir.
--pack-out y --pack-dir son incompatibles.
--pack-copies puede ser 0 o mayor.
Los valores de paralelismo, tamaño de mensaje y timeout deben ser positivos.
```

Cuando se distribuye con copias deseadas mayores que 0, Stopan registra una publicación local en metadata para que `discover` y `verify` puedan saber cuántas copias esperaba ese pack.

### `metadata pack discover`

Uso:

```bash
stopan metadata pack discover [opciones]
```

Opciones:

```bash
--owner-id OWNER
--identity-file FILE
--membership-seed HOST:PORT
--target-parallelism N
--rpc-timeout-s SECONDS
--max-message-bytes BYTES
--max-candidates N
--show-sources
```

Lista metadata packs distribuidos para un owner. Consulta nodos remotos y agrupa copias por `pack_hash`.

`--target-parallelism`, `--rpc-timeout-s` y `--max-message-bytes` controlan cómo se hacen esas consultas remotas.

Si existe publicación local previa, muestra estado de presencia contra las copias deseadas. Si no existe publicación local para un pack, el estado queda como desconocido porque Stopan ve copias, pero no sabe cuál era el objetivo.

La consulta de publicaciones locales es auxiliar. Si la SQLite configurada no puede abrirse o consultarse, el comando continúa con el descubrimiento remoto, muestra una advertencia y clasifica los paquetes sin expectativa local como `UNKNOWN`.

### `metadata pack verify`

Uso:

```bash
stopan metadata pack verify [opciones]
```

Opciones:

```bash
--owner-id OWNER
--identity-file FILE
--membership-seed HOST:PORT
--target-parallelism N
--rpc-timeout-s SECONDS
--max-message-bytes BYTES
--pack-hash HASH
--all
--max-candidates N
--show-sources
```

Verifica presencia remota de metadata packs distribuidos. Con `--pack-hash` verifica un pack concreto. Con `--all` verifica todos los packs descubiertos hasta el límite de candidatos. Sin ambas opciones, verifica el latest descubierto.

`--target-parallelism`, `--rpc-timeout-s` y `--max-message-bytes` controlan las consultas remotas de verificación.

Validación: `--pack-hash` y `--all` son incompatibles.

Devuelve `0` si hay resultados y no hay fallos. Devuelve `1` si no hay resultados o si detecta fallos.

### `metadata pack recover`

Uso:

```bash
stopan metadata pack recover [opciones]
```

Opciones:

```bash
--owner-id OWNER
--identity-file FILE
--object-store DIR
--passphrase-file FILE
--membership-seed HOST:PORT
--target-parallelism N
--rpc-timeout-s SECONDS
--max-message-bytes BYTES
--download-dir DIR
--pack-out PATH
--download-only
--target-hash HASH
--vault-id VAULT_ID
--no-import-db
--no-protection
--default-desired-remote-copies N
--scrypt-n N
--scrypt-r N
--scrypt-p N
--metadata-key-length N
```

Recupera metadata desde packs distribuidos. Descubre packs remotos del owner, elige uno válido, lo descarga, valida firma/hash, lo importa al object store local y, por defecto, reconstruye el catálogo SQLite.

`--target-parallelism`, `--rpc-timeout-s` y `--max-message-bytes` controlan la búsqueda y la descarga remota. El pack se recibe por bloques, se valida mientras se escribe en un temporal y solo se publica tras comprobar su tamaño, firma y hash.

`--target-hash` fuerza un pack concreto.

`--vault-id` limita la selección automática a un vault concreto cuando el owner tiene packs de varios vaults.

`--download-only` descarga y valida íntegramente el pack elegido, incluida la correspondencia entre hash, tipo, tamaño y bytes canónicos de cada objeto. No importa el object store ni reconstruye la DB.

`--no-import-db` importa el pack al object store local pero no reconstruye el catálogo SQLite.

`--no-protection` reconstruye DB sin importar `chunk_protection`, creando filas `PENDING` para chunks conocidos. `--default-desired-remote-copies` decide cuántas copias deseadas tendrán esas filas si hace falta.

Validaciones relevantes:

```text
--pack-out y --download-dir son incompatibles.
--download-only es incompatible con --no-import-db.
--download-only es incompatible con --no-protection.
--download-only es incompatible con --default-desired-remote-copies.
--no-import-db es incompatible con --no-protection.
--no-import-db es incompatible con --default-desired-remote-copies.
```

Si no hay `--object-store` ni `metadata.object_store_dir`, `--download-only` todavía puede funcionar si se indica `--pack-out` o `--download-dir` para guardar el pack descargado.

### `metadata pack local-store`

Uso:

```bash
stopan metadata pack local-store [opciones] PATH
```

Argumento:

```text
PATH  ruta del fichero .stopanmetapack
```

Opciones:

```bash
--owner-id OWNER
--identity-file FILE
--pack-store DIR
--expected-pack-hash HASH
--passphrase-file FILE
```

Guarda un metadata pack cifrado en el distributed pack store local por `owner_id`. Calcula el hash del fichero, lo firma con la identity private key y lo deja disponible para que el nodo pueda servirlo por RPC.

Si se pasa `--expected-pack-hash`, lo compara con el hash calculado del fichero.

### `metadata pack local-list`

Uso:

```bash
stopan metadata pack local-list [opciones]
```

Opciones:

```bash
--owner-id OWNER
--identity-file FILE
--pack-store DIR
```

Lista metadata packs guardados localmente en el distributed pack store para un owner.

### `metadata pack local-retrieve`

Uso:

```bash
stopan metadata pack local-retrieve [opciones]
```

Opciones:

```bash
--owner-id OWNER
--identity-file FILE
--pack-store DIR
--pack-hash HASH
--out PATH
```

Extrae un metadata pack del distributed pack store local a una ruta concreta y verifica que el contenido recuperado coincide con `--pack-hash`.

`--pack-hash` y `--out` son obligatorios.

## `stopan gc`

Uso general:

```bash
stopan gc TARGET [opciones]
```

Todos los targets de GC son locales. Por defecto se ejecutan en dry-run: muestran qué borrarían sin borrar nada. Para borrar de verdad hay que pasar `--apply`.

Flags comunes de ejecución:

```bash
--dry-run
--apply
```

`--dry-run` y `--apply` son incompatibles entre sí.

La sección `gc` de `stopan.yaml` define edades y periodos de gracia. Esa configuración no convierte el CLI manual en modo apply por defecto.

### Targets de chunks y shards

```bash
stopan gc generated-chunks [opciones]
stopan gc received-chunks [opciones]
stopan gc received-ec [opciones]
```

Opciones de `generated-chunks`:

```bash
--chunk-store DIR
--grace-hours HOURS
--dry-run
--apply
```

Opciones de `received-chunks`:

```bash
--chunk-store DIR
--max-age-days DAYS
--dry-run
--apply
```

Opciones de `received-ec`:

```bash
--ec-store DIR
--max-age-days DAYS
--dry-run
--apply
```

`generated-chunks` limpia chunks propios en `storage.local_chunk_dir`. Usa periodo de gracia en horas.

`received-chunks` limpia chunks recibidos bajo `storage.custody_dir/chunks`. Usa edad máxima en días. Valor `0` desactiva el borrado por edad.

`received-ec` limpia shards EC recibidos bajo `storage.custody_dir/ec_shards`. Usa edad máxima en días. Valor `0` desactiva el borrado por edad. Los shards EC creados para un push no se persisten localmente y no tienen un target `generated-ec`.

### Targets de metadata generada

```bash
stopan gc generated-metadata-graph [opciones]
stopan gc generated-metadata-packs [opciones]
```

Opciones de `generated-metadata-graph`:

```bash
--object-store DIR
--passphrase-file FILE
--object-grace-hours HOURS
--dry-run
--apply
```

Opciones de `generated-metadata-packs`:

```bash
--object-store DIR
--passphrase-file FILE
--identity-file FILE
--pack-grace-hours HOURS
--pack-dir DIR
--dry-run
--apply
```

`generated-metadata-graph` limpia objetos huérfanos del metadata object store generado localmente.

`generated-metadata-packs` limpia `.stopanmetapack` locales obsoletos.

### Targets de metadata recibida o recuperada

```bash
stopan gc received-metadata-packs [opciones]
stopan gc recovered-metadata-packs [opciones]
```

Opciones de `received-metadata-packs`:

```bash
--pack-store DIR
--max-age-days DAYS
--dry-run
--apply
```

Opciones de `recovered-metadata-packs`:

```bash
--object-store DIR
--pack-dir DIR
--max-age-days DAYS
--dry-run
--apply
```

`received-metadata-packs` limpia packs recibidos en `metadata.custody_pack_store_dir` aplicando límites de edad y cuotas del pack store.

`recovered-metadata-packs` limpia packs descargados por `metadata pack recover`. Por defecto usa `metadata.recovered_pack_dir`.

### Target global

```bash
stopan gc all [opciones]
```

Opciones:

```bash
--passphrase-file FILE
--identity-file FILE
--dry-run
--apply
```

`all` ejecuta todos los GC locales configurados en este orden:

```text
generated-chunks
received-chunks
received-ec
generated-metadata-graph
generated-metadata-packs
received-metadata-packs
recovered-metadata-packs
```

Ejemplos:

```bash
stopan gc all --dry-run
stopan gc all --apply
```

Validaciones relevantes:

```text
--max-age-days puede ser 0 o mayor.
--grace-hours puede ser 0 o mayor.
--object-grace-hours puede ser 0 o mayor.
--pack-grace-hours puede ser 0 o mayor.
```

## Resumen de comandos que modifican estado

Modifican metadata o stores locales/remotos:

```text
backup
push
verify
restore
node
init node
init metadata
metadata graph export
metadata graph import
metadata pack create
metadata pack import
metadata pack push
metadata pack recover
metadata pack local-store
metadata pack local-retrieve
config example --out
gc TARGET --apply
```

Solo consultan o validan cuando se usan sin salida destructiva:

```text
config example sin --out
config validate
metadata status
metadata identity-show
metadata graph status
metadata pack inspect
metadata pack list
metadata pack discover
metadata pack verify
metadata pack local-list
node status
gc TARGET --dry-run
```

`restore` modifica el directorio de salida, no la protección remota.

`verify` puede modificar metadata porque actualiza estados de verificación o degradación y puede disparar exportación automática del metadata object graph si está configurada o se fuerza por CLI.
