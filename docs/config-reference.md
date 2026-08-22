# Referencia de configuración

Este documento describe el fichero principal de configuración de Stopan. En una instalación con paquete, el fichero esperado es:

```text
/etc/stopan/stopan.yaml
```

El ejemplo instalado por el paquete se conserva en:

```text
/usr/share/doc/stopan/examples/configs/stopan.yaml
```

También puede generarse con:

```bash
stopan config example
```

Y validarse con:

```bash
stopan config validate /etc/stopan/stopan.yaml
```

## Precedencia y validación

Stopan aplica primero los defaults internos y después sobrescribe con el YAML. Los argumentos explícitos del CLI se aplican por encima de la configuración cargada.

La mayoría de comandos usan `/etc/stopan/stopan.yaml` por defecto. Si ese fichero no existe, los comandos operativos fallan con un mensaje indicando que debe inicializarse la máquina con `stopan init node` o pasarse otro fichero mediante `--config`.

El loader de configuración es estricto: rechaza secciones desconocidas, campos desconocidos y valores con tipos inválidos. Esto evita que un error de nombre deje una opción crítica sin efecto. `cluster.seeds` acepta una lista YAML, una tupla interna o un string separado por comas. Los booleanos deben ser booleanos YAML reales (`true`/`false`), no strings.

`stopan config validate` comprueba estructura, tipos y rangos de valores. No comprueba que los nodos remotos estén vivos ni que la passphrase de metadata pueda descifrar la identidad.

## Sección `node`

`node` define la identidad operativa del nodo, su catálogo y las direcciones usadas por el proceso persistente.

`bind_addr` es la dirección donde escucha el servidor gRPC local. En máquinas con IPv4 e IPv6 suele bastar con `[::]:50051`.

`advertise_addr` es la dirección que otros nodos deben usar para contactar con este nodo. Debe ser alcanzable desde el resto del cluster. El servicio `stopan node` requiere que esté configurada para arrancar.

`identity_file` contiene la identidad operativa estable del nodo y su `incarnation`.

`catalog_file` es el catálogo SQLite local. Contiene snapshots, items, recipes, chunks conocidos, estados de protección, data packs EC y publicaciones locales de metadata packs.

Ejemplo:

```yaml
node:
  bind_addr: "[::]:50051"
  advertise_addr: "node1.lan:50051"
  identity_file: "/var/lib/stopan/state/node_id.txt"
  catalog_file: "/var/lib/stopan/state/catalog.sqlite"
```

## Sección `cluster`

`cluster` configura la pertenencia lógica a la red.

`token` es un token compartido por los nodos que deben verse entre sí. Los RPC de membership, almacenamiento y metadata packs lo usan como barrera lógica. `stopan node` exige que sea no vacío y rechaza el arranque si la credencial no está configurada. No sustituye a TLS, VPN, firewall ni control de red.

`seeds` es la lista de direcciones iniciales usadas para descubrir miembros. El nodo intenta contactarlas al arrancar y, si existen seeds externos pero todavía no ha descubierto ningún par, reintenta la incorporación periódicamente hasta conseguirlo. Después el mantenimiento de la vista queda en manos del protocolo de membership y los seeds dejan de utilizarse como mecanismo de reparación. En una instalación estable conviene poner varios nodos que normalmente estén encendidos. Si `stopan init node` se ejecuta sin `--seed`, escribe como seed el propio `advertise_addr`, que se ignora como contacto externo y permite arrancar un primer nodo aislado.

Ejemplo:

```yaml
cluster:
  token: "change-me"
  seeds:
    - "node1.lan:50051"
    - "node2.lan:50051"
```

## Sección `protection`

`protection` define la política por defecto de protección remota de datos. No ejecuta protección por sí sola: `backup` solo registra la intención y `push` realiza el envío real.

`remote_copies` es el número de copias remotas completas deseadas por chunk en modo `replication`. La copia local del origen no cuenta como copia remota.

`strict_remote_copies` decide si una operación debe fallar cuando no hay suficientes nodos remotos elegibles. Con `false`, puede avanzar parcialmente y dejar estado degradado para reintentos posteriores. Con `true`, exige capacidad suficiente para cumplir la política.

`ec_k` y `ec_m` configuran el modo EC. `ec_k` es el número de shards de datos necesarios y `ec_m` el número de shards de paridad. Para proteger datos con EC hacen falta al menos `ec_k + ec_m` nodos remotos elegibles, porque el origen queda excluido del placement.

`ec_pack_size_bytes` controla el tamaño objetivo de los data packs EC antes de generar shards. Debe ser compatible con los límites gRPC y de almacenamiento configurados.

Ejemplo:

```yaml
protection:
  remote_copies: 3
  strict_remote_copies: false
  ec_k: 2
  ec_m: 1
  ec_pack_size_bytes: 8388608
```

## Sección `backup`

`backup.workers` define el número de workers por defecto para `stopan backup`. El CLI puede sobrescribirlo con el argumento posicional `workers`.

El proceso limita el número efectivo de workers al número de CPUs expuestas por el sistema. Si se pide más, Stopan informa y reduce el valor.

Ejemplo:

```yaml
backup:
  workers: 4
```

## Sección `grpc`

`grpc` contiene parámetros comunes para canales gRPC.

`max_message_bytes` limita cada mensaje gRPC. Debe estar alineado con `storage.max_chunk_size` y con los shards que se envían en un único mensaje. Los metadata packs se transfieren como un flujo de bloques y su tamaño total queda limitado por `metadata.max_distributed_pack_bytes`.

`keepalive_time_ms`, `keepalive_timeout_ms` y `keepalive_permit_without_calls` ajustan el comportamiento de keepalive de los canales. Estos valores solo suelen modificarse por requisitos de red o infraestructura.

Ejemplo:

```yaml
grpc:
  max_message_bytes: 8388608
  keepalive_time_ms: 120000
  keepalive_timeout_ms: 20000
  keepalive_permit_without_calls: false
```

## Sección `storage`

`storage` configura los repositorios de datos y los límites del servicio que recibe contenido remoto.

`local_chunk_dir` es el CAS local de chunks propios. `backup` materializa aquí los chunks que necesita conservar localmente y `restore` lo consulta como primera fuente.

`custody_dir` es la raíz del contenido recibido de otros nodos. El servicio deriva de ella `chunks/` para fragmentos completos y `ec_shards/` para fragmentos codificados.

`rpc_workers` controla la concurrencia del servidor gRPC.

`commit_workers` y `commit_queue_items` controlan la cola interna que materializa escrituras de chunks recibidos.

`max_chunk_size` es el tamaño máximo aceptado para chunks o payloads de almacenamiento. Debe mantenerse coherente con `grpc.max_message_bytes`.

Ejemplo:

```yaml
storage:
  local_chunk_dir: "/var/lib/stopan/data/chunks"
  custody_dir: "/var/lib/stopan/custody"
  rpc_workers: 64
  commit_workers: 16
  commit_queue_items: 256
  max_chunk_size: 8388608
```

## Sección `replication`

`replication` ajusta el modo `push --protection-mode replication`.

`target_parallelism` limita cuántos targets remotos se procesan en paralelo.

`probe_batch_hashes` define cuántos hashes se consultan por lote cuando Stopan pregunta a un nodo qué chunks le faltan.

`stream_inflight` limita los envíos pendientes dentro del stream de replicación.

`probe_timeout_s` y `stream_timeout_s` son timeouts separados para la fase de probing y la fase de envío.

`commit_every` controla cada cuántas confirmaciones se consolidan cambios de estado en metadata durante el push.

Ejemplo:

```yaml
replication:
  target_parallelism: 4
  probe_batch_hashes: 2048
  stream_inflight: 64
  probe_timeout_s: 10.0
  stream_timeout_s: 60.0
  commit_every: 100
```

## Sección `verify`

`verify` ajusta las verificaciones remotas.

`target_parallelism` limita los targets consultados en paralelo.

`probe_batch_hashes` limita los chunks o shards incluidos en cada consulta remota de verificación.

`probe_timeout_s` es el timeout de las consultas remotas de presencia.

`verify` no descarga blobs completos: consulta evidencia de presencia remota y actualiza el estado de protección en metadata.

Ejemplo:

```yaml
verify:
  target_parallelism: 4
  probe_batch_hashes: 2048
  probe_timeout_s: 10.0
```

## Sección `restore`

`restore` ajusta la restauración de datos.

`batch_target_parallelism` limita cuántos targets remotos se consultan en paralelo por ronda cuando el restore usa red.

`prefetch_window` define cuántos chunks se resuelven por ventana de lectura anticipada.

`rpc_timeout_s` es el timeout de recuperación remota de chunks o shards.

El modo de recuperación remota no se define aquí. El CLI usa `--remote-recovery`, cuyo default real es `none`. Por tanto, un restore normal no usa red salvo que se indique `replication`, `ec` o `auto`.

Ejemplo:

```yaml
restore:
  batch_target_parallelism: 4
  prefetch_window: 32
  rpc_timeout_s: 5.0
```

## Sección `membership`

`membership` ajusta el protocolo de descubrimiento y gossip.

`protocol_period_s` marca la cadencia del protocolo.

`bootstrap_retry_interval_s` marca el intervalo nominal inicial de incorporación mientras hay seeds externos configurados y todavía no se ha descubierto ningún par. Debe estar en el intervalo `(0, 30]` s. Tras fallos sucesivos, la espera crece exponencialmente hasta un máximo de 30 s y se introduce una pequeña variación aleatoria para evitar que varios nodos aislados reintenten de forma sincronizada. Cada reintento prueba un único seed y los contactos se recorren en round-robin.

`ping_timeout_s` es el timeout de ping directo.

`rpc_timeout_s` es el timeout para llamadas RPC de membership usadas por clientes y operaciones distribuidas.

`suspect_timeout_s` controla cuánto tiempo puede estar un nodo en sospecha antes de considerarse no disponible.

`indirect_ping_fanout` define cuántos nodos se usan para ping indirecto.

`max_gossip_events` y `gossip_ttl_s` limitan el volumen y vida de eventos difundidos.

Ejemplo:

```yaml
membership:
  protocol_period_s: 1.0
  bootstrap_retry_interval_s: 5.0
  ping_timeout_s: 0.25
  rpc_timeout_s: 2.0
  suspect_timeout_s: 6.0
  indirect_ping_fanout: 3
  max_gossip_events: 20
  gossip_ttl_s: 60.0
```

## Sección `metadata`

`metadata` configura la protección de metadata mediante object graph cifrado y metadata packs.

`passphrase_file` apunta al fichero privado con la passphrase usada para abrir la identidad y derivar claves.

`identity_file` apunta a la identidad privada de metadata. `stopan init metadata` la crea si no existe.

`owner_id` identifica al propietario de los metadata packs. Se calcula desde la identidad y se escribe en la configuración durante `stopan init metadata`. Si se configura manualmente, debe ser hexadecimal lowercase de 64 caracteres y debe coincidir con la identidad.

`object_graph_auto_export` activa export automático del metadata object graph en comandos que soportan auto-export, como `backup`, `push` y `verify`, salvo que el CLI lo sobrescriba.

`object_store_dir` es el metadata object store cifrado. Contiene los objetos del graph, sus punteros de estado y el contador de generaciones del vault.

`object_graph_include_protection` decide si el graph incluye estado de protección remoto. Para recuperación completa de snapshots y política distribuida, normalmente debe quedar en `true`.

`object_graph_auto_pack` crea también un metadata pack cuando se hace auto-export del graph.

`generated_pack_dir` es el directorio de packs generados localmente.

`recovered_pack_dir` es el directorio donde se conservan los packs descargados durante recuperación.

`custody_pack_store_dir` es el store donde el servicio remoto conserva metadata packs recibidos de otros nodos.

`pack_copies` es el número de copias remotas deseadas para metadata packs.

`strict_pack_copies` exige que haya capacidad suficiente para cumplir `pack_copies`.

`pack_discovery_max_candidates` limita cuántos packs candidatos se consideran al descubrir packs remotos.

`pack_target_parallelism` limita cuántos targets de metadata pack se procesan en paralelo.

`pack_rpc_timeout_s` es el timeout RPC de push, discover, verify y recover de metadata packs.

`cli_warning_limit` limita cuántos avisos o resultados se muestran en algunas salidas CLI de metadata.

Los límites `max_distributed_pack_bytes`, `max_distributed_packs_per_owner`, `max_distributed_pack_bytes_per_owner` y `max_distributed_pack_store_bytes` protegen el store de packs recibidos frente a abuso o crecimiento descontrolado. `max_distributed_pack_bytes` se aplica al paquete completo, aunque su publicación y recuperación se realicen mediante bloques gRPC. Los límites por owner y por store deben ser al menos tan grandes como el tamaño máximo de un pack individual.

`scrypt_n`, `scrypt_r`, `scrypt_p` y `key_length` controlan la derivación criptográfica. `scrypt_n` debe ser potencia de dos y `key_length` debe ser al menos 32.

Ejemplo:

```yaml
metadata:
  passphrase_file: "/etc/stopan/metadata.passphrase"
  owner_id: ""
  identity_file: "/etc/stopan/metadata_identity.json"
  object_graph_auto_export: false
  object_store_dir: "/var/lib/stopan/metadata/object_store"
  object_graph_include_protection: true
  object_graph_auto_pack: false
  generated_pack_dir: "/var/lib/stopan/metadata/packs/generated"
  recovered_pack_dir: "/var/lib/stopan/metadata/packs/recovered"
  custody_pack_store_dir: "/var/lib/stopan/custody/metadata_packs"
  pack_copies: 3
  strict_pack_copies: false
  pack_discovery_max_candidates: 10
  pack_target_parallelism: 4
  pack_rpc_timeout_s: 60.0
  cli_warning_limit: 10
  max_distributed_pack_bytes: 67108864
  max_distributed_packs_per_owner: 8
  max_distributed_pack_bytes_per_owner: 536870912
  max_distributed_pack_store_bytes: 10737418240
  scrypt_n: 32768
  scrypt_r: 8
  scrypt_p: 1
  key_length: 32
```

## Sección `gc`

`gc` configura los targets de limpieza local. El CLI `stopan gc` es dry-run por defecto y solo borra con `--apply`.

Los campos `generated_*_grace_hours` son periodos de gracia para artefactos generados localmente. Un valor `0` permite considerar inmediatamente los ficheros antiguos según el target.

Los campos `received_*_max_age_days` y `recovered_metadata_pack_max_age_days` son políticas de edad para artefactos recibidos o recuperados. En los targets de edad máxima, `0` desactiva el borrado por edad.

`generated_chunk_grace_hours` se aplica a chunks propios en `storage.local_chunk_dir` que no estén asociados a snapshots `CREATING` o `COMPLETE`. La alcanzabilidad desde esas capturas prevalece sobre el periodo de gracia. Un valor `0` permite considerar inmediatamente solo los chunks no protegidos por esa condición.

`received_chunk_max_age_days` se aplica a chunks recibidos bajo `storage.custody_dir/chunks`.

`received_ec_max_age_days` se aplica a shards EC recibidos bajo `storage.custody_dir/ec_shards`. Los shards EC generados para un push no forman una familia persistente local y no tienen un target de GC propio.

`generated_metadata_graph_grace_hours` se aplica a objetos huérfanos del metadata object graph.

`generated_metadata_pack_grace_hours` se aplica a metadata packs generados localmente que ya no se necesitan.

`received_metadata_pack_max_age_days` se aplica al store de packs recibidos por el servicio remoto.

`recovered_metadata_pack_max_age_days` se aplica a packs descargados durante recuperación.

Ejemplo:

```yaml
gc:
  generated_chunk_grace_hours: 48.0
  received_chunk_max_age_days: 0
  received_ec_max_age_days: 0
  generated_metadata_graph_grace_hours: 48.0
  generated_metadata_pack_grace_hours: 48.0
  received_metadata_pack_max_age_days: 0
  recovered_metadata_pack_max_age_days: 0
```

## Claves sensibles

`cluster.token`, `metadata.passphrase_file` y `metadata.identity_file` deben tratarse como secretos operativos.

`owner_id` identifica al propietario de metadata packs, pero no permite descifrarlos. Para crear, validar o recuperar packs hacen falta la identidad de metadata y la passphrase asociada.

Para ejemplos completos de configuración en varios nodos, consulta [`docs/local-network-deployment.md`](local-network-deployment.md).
