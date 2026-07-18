# Arquitectura

Este documento describe el diseño interno del código.

## Objetivo del sistema

Stopan es un sistema de backup distribuido basado en direccionamiento por contenido. Su diseño separa tres responsabilidades:

- capturar snapshots locales de un árbol de ficheros
- proteger datos y metadata fuera del nodo origen
- reconstruir snapshots a partir de metadata, CAS local y nodos remotos

El backup es local por diseño. Crea snapshots, recipes, chunks y metadata. La protección remota se materializa después mediante `push`. Esta separación permite crear backups aunque el clúster no esté disponible, y permite reintentar protección remota sin repetir el recorrido del árbol original.

## Principios de diseño

Stopan sigue varios principios:

- Los datos se identifican por el hash BLAKE3 de su contenido raw.
- El CAS guarda blobs comprimidos, pero el hash siempre corresponde al contenido sin comprimir.
- La metadata SQLite es el plano de control local.
- La copia local del origen no cuenta como copia remota.
- El `origin_node_id` se excluye del placement remoto.
- La protección remota es explícita y queda registrada en metadata.
- `backup`, `push` y `verify` pueden actualizar el metadata object graph tras modificar metadata.
- La recuperación depende de conservar metadata y de que los datos se hubieran protegido antes del fallo.
- Los nodos remotos no necesitan descifrar metadata packs para almacenarlos.

## Organización del código

El árbol principal vive bajo `src/stopan`.

`cli` contiene la capa de entrada. Parsean argumentos, aplican validaciones de uso, cargan configuración y delegan en servicios de dominio.

`config` define el modelo de `stopan.yaml`, defaults, loader estricto y generación de ejemplos.

`backup`, `scanning`, `chunking` y `cas` forman el flujo de captura local. Recorren el árbol, dividen archivos en chunks, escriben el CAS y registran recipes.

`metadata.database` contiene el modelo SQLite operacional. Es la fuente de verdad local para snapshots, recipes, chunks, protección remota, data packs EC y publicaciones de metadata packs.

`cluster`, `node.membership` y `placement` separan discovery, vista canónica de nodos y selección determinista de targets.

`node` implementa el proceso servidor. Expone membership, almacenamiento P2P, shards EC y metadata packs sobre gRPC.

`protection.replication` y `protection.ec` implementan los dos modos de protección remota de datos.

`restore` reconstruye snapshots usando metadata y una cadena de fuentes de chunks.

`metadata.objects`, `metadata.packs`, `metadata.identity` y `metadata.crypto` implementan protección de metadata, object graph cifrado, packs cifrados y firmados, distribución y recuperación.

`gc` contiene limpieza local por familias de artefactos.

`jobs` adapta perfiles YAML periódicos a scripts de sistema. No define una arquitectura de backup distinta, solo automatiza comandos existentes.

## Capas persistentes

Stopan trabaja con varias capas persistentes separadas.

El CAS local principal está configurado por `node.local_shard_dir`. Guarda chunks generados por backup, comprimidos con Zstandard y direccionados por BLAKE3. Es la primera fuente que consulta restore.

La metadata SQLite está configurada por `node.db_file`. Guarda snapshots, items, recipes, chunks conocidos, estado de protección, data packs EC y publicaciones locales de metadata packs.

El store P2P del nodo está configurado por `node.repo_store_dir` Guarda la identidad del nodo en `node_id.txt`, chunks recibidos por replicación y shards EC recibidos por el servicio remoto.

La metadata distribuida usa tres rutas independientes: `metadata.object_store_dir` para el object graph cifrado, `metadata.object_pack_dir` para packs generados localmente y `metadata.distributed_pack_store_dir` para packs recibidos por el servicio remoto.

Esta separación evita mezclar el CAS generado por el nodo origen con los datos recibidos como servidor P2P y permite aplicar GC distinto a cada familia.

## Snapshots, items y recipes

Un snapshot representa una captura de una raíz local. Se crea en estado `CREATING`, pasa a `COMPLETE` si todos los archivos se procesan correctamente y queda en `FAILED` si ocurre un error.

El recorrido del árbol lo realiza `TreeWalker`. Emite la raíz como `.` y produce items de tipo `dir` y `file`. Ignora symlinks y ficheros especiales. Puede recorrer de forma determinista para pruebas o comparaciones reproducibles.

Cada archivo se reconstruye mediante una recipe. Una recipe es una secuencia ordenada de chunks, donde cada entrada contiene orden, hash y tamaño. El `recipe_hash` se calcula con SHA-256 sobre esa secuencia canónica. El contenido de cada chunk queda identificado aparte por BLAKE3.

Si un archivo no cambia respecto al snapshot completo anterior de la misma raíz, se puede reutilizar la recipe anterior y evitar leer de nuevo el archivo completo. La comprobación usa metadata de filesystem como tamaño, modo, propietario, grupo y mtime.

## Chunking y CAS

`FileChunker` divide archivos con Content-Defined Chunking usando una extensión nativa basada en Rabin. El tamaño medio por defecto es 65536 bytes, con mínimo 16384 y máximo 262144.

El chunker usa `mmap` para leer archivos sin cargarlos completos en memoria. Cada chunk se identifica con BLAKE3 sobre bytes raw.

`CASRepository` comprime chunks con Zstandard y escribe cada blob bajo una ruta derivada del hash. Las escrituras usan fichero temporal y `rename` atómico. Al leer con `get`, el CAS descomprime y valida que el BLAKE3 calculado coincide con el hash pedido.

El CAS también puede guardar blobs comprimidos ya validados. Esto se usa cuando restore recupera un chunk remoto, lo valida en memoria y lo cachea en el CAS local.

## Backup local

El servicio de backup coordina el recorrido del árbol, la creación del snapshot, los workers de archivos y la escritura final de metadata.

Primero resuelve un `origin_node_id`. Si puede obtener una vista de membership y localizar el nodo actual, usa el `node_id` del clúster. Si no, usa una identidad local persistida en `node_id.txt`.

Después construye la política de fast-path. `safe_mode` desactiva cualquier salto. El fast-path local permite saltar chunks ya presentes en el CAS local. El fast-path remoto permite saltar chunks cuya fila `chunk_protection` acredita protección suficiente para el `placement_epoch` actual.

Los workers procesan archivos y materializan chunks en CAS cuando la política lo exige. No escriben items del snapshot. Devuelven la recipe y estadísticas al coordinador, que actualiza SQLite.

Al cerrar correctamente el snapshot, el servicio puede exportar el metadata object graph si la configuración o el CLI lo han pedido.

Backup no envía chunks a nodos remotos. Solo registra el objetivo de protección deseado para que `push` y `verify` puedan operar después.

## Metadata SQLite

`MetadataDB` inicializa el esquema operacional y activa SQLite en modo WAL.

Las tablas principales son:

- `metadata_vault`, con el `vault_id` lógico de la metadata
- `snapshots`, con UUID, raíz, `origin_node_id`, estado y contadores
- `snapshot_items`, con rutas, tipo de item, metadata de filesystem y recipe asociada
- `recipes` y `recipe_chunks`, con la secuencia necesaria para reconstruir archivos
- `chunks`, con hash, tamaño y contador de referencias
- `chunk_protection`, fuente de verdad del modo replication
- `erasure_data_packs`, `erasure_data_pack_chunks` y `erasure_data_pack_shards`, fuente de verdad del modo EC
- `metadata_pack_publications`, con publicaciones locales de metadata packs

`chunk_protection` no representa EC. Replication y EC mantienen modelos separados.

Los estados comunes de protección son `PENDING`, `PLACED`, `DEGRADED`, `VERIFIED` y `FAILED`. Solo `PLACED` y `VERIFIED` cuentan como evidencia suficiente para saltos rápidos o para considerar una protección vigente.

Una evidencia de protección remota solo es suficiente si cubre las copias remotas requeridas, tiene suficientes copias confirmadas y, cuando aplica, coincide con el `placement_epoch` actual.

## Nodo P2P

`stopan node` arranca el proceso servidor. Requiere `node.advertise_addr`, porque el nodo debe poder identificarse dentro del clúster.

En el arranque se cargan o crean dos datos persistentes de identidad: `node_id` e `incarnation`. El archivo `node_id.txt` contiene exactamente esas dos líneas. En cada arranque se incrementa `incarnation`, para que membership pueda distinguir una ejecución nueva de una anterior.

El servidor gRPC registra tres servicios:

`P2PStorage` expone chunks y shards EC. Sus RPC principales son `ProbeMissingChunks`, `ReplicateChunks`, `RetrieveChunkBatch`, `ProbeMissingDataPackShards`, `ReplicateDataPackShards` y `RetrieveDataPackShardBatch`. Los clientes adjuntan `cluster.token` como metadata binaria de gRPC y el servidor rechaza la llamada antes de consultar, leer o escribir contenido cuando la credencial configurada no coincide.

`Membership` expone `Join`, `Ping`, `PingReq` y `GetMembers`.

`MetadataPackService` expone `StoreMetadataPack`, `ListMetadataPacks`, `RetrieveMetadataPack` y `ProbeMetadataPack`.

El almacenamiento remoto de chunks pasa por un commit engine interno con cola y workers. El servicio valida tamaño, hash y consistencia antes de aceptar blobs. Los shards EC usan un store separado bajo el store P2P del nodo.

## Membership y vista de clúster

El proceso de nodo mantiene membership con un protocolo tipo SWIM. Los clientes operativos no mantienen membership permanente. Para `push`, `verify`, restore remoto o metadata packs, resuelven una vista del clúster en el momento de la operación.

`ClusterView` representa la vista canónica usada por placement. Contiene miembros elegibles y, si se pudo resolver, el `self_node_id`.

El token de clúster forma parte de la separación lógica entre clústeres. Membership y metadata packs lo transportan dentro de sus mensajes. `P2PStorage` lo transporta mediante metadata binaria de gRPC para cubrir de forma uniforme las consultas de presencia, la replicación y la recuperación de chunks y shards EC. Un token vacío desactiva esta barrera lógica. El mecanismo no identifica criptográficamente al nodo y no sustituye a TLS, una VPN, un cortafuegos ni otro control de red.

## Placement remoto

Stopan usa HRW (Rendezvous Hashing) para seleccionar targets remotos de forma determinista.

La puntuación HRW se calcula con BLAKE3 sobre una combinación estable de `cluster_token`, `node_id` y hash de objeto. Para un mismo conjunto de nodos y política, todos los clientes pueden calcular los mismos targets sin mantener una tabla de asignación global.

El nodo origen se excluye del placement remoto. Esto afecta a replication, EC, verificación y recuperación remota.

`placement_epoch` identifica el contexto de placement vigente. Cambia si cambia el token, el número de copias remotas requeridas o el conjunto de nodos elegibles. Se usa para detectar evidencia antigua que ya no corresponde al placement actual.

## Protección por replication

Replication protege chunks completos en nodos remotos.

El flujo de `push` en replication es:

- resolver membership y `origin_node_id`
- calcular `placement_epoch` excluyendo el origen
- seleccionar chunks candidatos según scope
- planificar targets HRW por chunk
- consultar inventario remoto con `ProbeMissingChunks`
- enviar chunks ausentes con `ReplicateChunks`
- actualizar `chunk_protection`

Si el modo estricto está activo y no hay suficientes nodos remotos elegibles, la operación aborta sin marcar protección incompleta como válida.

Un chunk queda `PLACED` si tiene todas las copias remotas requeridas, `DEGRADED` si hay progreso parcial y `FAILED` si no se consiguió ninguna copia útil.

La verificación de replication no descarga blobs completos. Recalcula los targets HRW vigentes y consulta presencia con `ProbeMissingChunks`. Si todas las copias requeridas están presentes, marca `VERIFIED`. Si no, marca `DEGRADED`.

## Protección por erasure coding

El modo EC no replica chunks sueltos. Agrupa chunks deduplicados en data packs, calcula un hash del payload del pack y genera shards mediante `zfec`.

Cada data pack tiene `ec_k` data shards y `ec_m` parity shards. Para colocar un pack hacen falta `ec_k + ec_m` nodos remotos elegibles distintos.

El flujo de `push` EC es:

- resolver membership y comprobar capacidad remota
- buscar packs pendientes de reintento
- buscar chunks nuevos según scope
- agrupar chunks en data packs hasta el tamaño objetivo
- generar shards
- colocar cada shard en un nodo remoto distinto
- registrar pack, chunks del pack y shards en SQLite
- marcar el estado del pack

`ec_m = 0` es válido, pero no aporta redundancia. En ese caso hacen falta todos los shards para reconstruir el pack.

Un data pack queda `PLACED` si están todos sus shards, `DEGRADED` si faltan shards pero todavía hay al menos `ec_k` shards recuperables y `FAILED` si quedan menos de `ec_k`.

La verificación EC usa `ProbeMissingDataPackShards`. No reconstruye el pack. Solo comprueba presencia de shards y actualiza el estado persistido.

## Scopes de protección

`push` y `verify` comparten scopes para acotar el conjunto de chunks o packs:

`pending` trabaja sobre elementos pendientes, degradados, fallidos o asociados a un epoch obsoleto.

`snapshot` limita la operación a chunks alcanzables desde un snapshot completo concreto.

`all-reachable` trabaja sobre chunks alcanzables desde snapshots completos.

`all-known-chunks` trabaja sobre todos los chunks conocidos por metadata.

Estos scopes pertenecen al plano de control. No cambian el formato persistente ni el algoritmo de placement.

## Restore

Restore reconstruye un snapshot a partir de metadata y chunks raw.

`SnapshotRestorer` no decide de dónde salen los chunks. Esa decisión pertenece a `ChunkFetchService`.

La cadena de lectura es:

- CAS local principal
- CAS P2P local del nodo
- réplicas remotas con `RetrieveChunkBatch`, si la recuperación por replication está habilitada
- reconstrucción EC con `RetrieveDataPackShardBatch`, si la recuperación por EC está habilitada

La resolución de membership en restore es lazy. El clúster solo se resuelve cuando falta un chunk local y el modo de recuperación necesita red.

Todo chunk recuperado fuera del CAS local principal se descomprime con Zstandard y se valida con BLAKE3 antes de usarse. Si se valida correctamente, se intenta cachear en el CAS local para evitar repetir trabajo.

La recuperación EC trabaja a nivel de data pack completo. Cuando necesita un chunk de un pack, recupera al menos `ec_k` shards, reconstruye el payload completo, extrae todos los chunks del pack y cachea los chunks reconstruidos.

La escritura del restore usa un directorio `snapshot_<uuid>.incomplete`. Cada archivo se escribe primero como `.tmp`. Al completar todo el árbol, el directorio incompleto se promueve a `snapshot_<uuid>`. Si el destino final ya existe, la operación se rechaza sin sobrescribirlo ni asumir que contiene una restauración válida.

Las rutas restauradas pasan por una normalización que rechaza rutas absolutas, escapes con `..` y cualquier path que salga del directorio destino.

## Metadata object graph

La metadata distribuida no copia `_metadata.db` como fichero SQLite. Exporta el estado operacional a un metadata object graph cifrado.

El exporter construye objetos canónicos para snapshots, árboles, archivos, recipes, chunks conocidos, protection index y, si se incluye protección, data packs EC. Los hashes de objeto se calculan sobre bytes canónicos plaintext antes de cifrar o almacenar.

El object store mantiene un latest pointer. Ese latest apunta al catálogo vigente y permite importar de nuevo el estado a una base SQLite vacía.

El graph puede incluir o no estado de protección. Si se importa sin protección, se crean filas `PENDING` para chunks conocidos usando una política de copias remotas por defecto.

`backup`, `push` y `verify` pueden disparar auto-export después de modificar metadata. Así el estado protegible de metadata puede seguir al estado real de snapshots y protección remota.

## Metadata packs

Un `.stopanmetapack` empaqueta el latest del metadata object graph. El pack está cifrado para la identidad de metadata y se firma con la clave privada del owner.

El cliente que crea, descifra o recupera packs necesita `metadata_identity.json` y la passphrase. Los nodos remotos no necesitan esos secretos. Solo guardan el payload cifrado y sidecars de validación.

El servicio remoto valida owner, hash, tamaño, clave pública, firma y cuotas antes de almacenar un pack.

La distribución de packs usa una política separada de la protección de chunks. `pack_copies` no es `remote_copies`.

Las publicaciones locales de metadata packs se guardan en SQLite para que discover y verify puedan comparar presencia remota contra intención de copias.

En `metadata pack discover`, esta comparación es de mejor esfuerzo. Un fallo al abrir o consultar la SQLite local no impide listar y validar referencias remotas. El comando informa de la incidencia y deja la presencia en `UNKNOWN` cuando no dispone de una expectativa local utilizable.

La recuperación de metadata lista packs remotos del owner, descarga candidatos, valida firma y hash, descifra con la identidad local y comprueba todos los objetos del paquete antes de considerarlo seleccionable. Después, salvo en `--download-only`, importa el pack al object store y, si se solicita, reconstruye `_metadata.db`.

## Verificación y estados

La verificación no busca demostrar que un blob remoto sea útil mediante descarga completa. Su función es auditar evidencia de presencia remota y actualizar metadata.

En replication, la evidencia se obtiene con probes de chunks esperados en targets HRW. En EC, la evidencia se obtiene con probes de shards registrados.

La validez de una protección depende de tres condiciones:

- suficientes copias o shards confirmados
- estado persistido compatible con protección
- `placement_epoch` vigente cuando aplica

Esto permite detectar datos que fueron protegidos bajo una vista anterior del clúster o una política distinta.

## GC y ciclo de vida de artefactos

El GC es local. No borra datos en otros nodos por coordinación distribuida.

Las familias principales son:

- chunks generados localmente
- chunks recibidos por replication
- shards EC generados localmente
- shards EC recibidos
- metadata object graph
- metadata packs generados
- metadata packs recibidos
- metadata packs recuperados

Los targets de edad máxima usan `0` como valor que desactiva borrado por edad. Los targets generados usan periodos de gracia para evitar borrar artefactos recién creados.

El metadata object graph usa un GC tipo mark-and-sweep desde `latest`. Los objetos alcanzables siguen vivos. Los objetos huérfanos se vuelven candidatos después del periodo de gracia. Los packs locales generados se tratan como artefactos de exportación o cache.

La limpieza conserva un opt-in destructivo tanto en la CLI como en los jobs periódicos. La omisión de `gc.apply` y los valores booleanos falsos se traducen a `--dry-run`. Solo un valor verdadero reconocido se traduce a `--apply`. Los valores no reconocibles se rechazan antes de invocar el CLI.

## Superficie de control

El CLI no contiene la lógica principal de backup, protección o restore. Su papel es cargar configuración, validar combinaciones y construir los servicios de dominio.

La configuración se carga como defaults internos más YAML. Los flags explícitos del CLI se aplican después. El loader rechaza secciones y campos desconocidos.

Los jobs periódicos no introducen un flujo alternativo. Traducen perfiles YAML a variables de entorno para scripts instalados, que a su vez llaman al CLI.
