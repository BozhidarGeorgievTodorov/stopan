# Stopan: mapa de arquitectura

Este documento fija el mapa mental del repositorio. No es una referencia de flags ni un manual de uso; para comandos concretos consultar `docs/cli-reference.md`.

## Regla principal

Stopan se organiza por dominio y caso de uso. La infraestructura común comparte mecánica; las reglas de negocio se quedan en su dominio.

```text
common/  -> primitivas puras sin vocabulario Stopan específico
rpc/     -> mecánica de transporte gRPC
cluster/ -> vista read-only del cluster para clientes
node/    -> runtime servidor y adaptadores RPC inbound
```

Los dominios conservan sus invariantes, nombres y excepciones propios aunque internamente compartan utilidades.

## Capas

```text
CLI
  ↓
Casos de uso cliente
  backup/
  restore/
  protection/
  metadata/packs/
  metadata/objects/
  ↓
Dominios persistentes
  cas/
  metadata/
  chunking/
  placement/
  ↓
Infraestructura compartida
  common/
  rpc/
  config/
  errors.py
  ↓
Nodo servidor
  node/services/
  node/storage/
  node/membership/
```

## Ownership de carpetas

### `common/`

Contiene utilidades puras reutilizables: escritura atómica, JSON canónico, validación BLAKE3 genérica, validadores escalares, secuencias y batching.

Los conceptos como `chunk_hash`, `pack_hash`, `metadata object`, `placement_epoch`, `RemoteShardRef` o estados de protección pertenecen a sus dominios.

### `rpc/`

Contiene mecánica gRPC compartida:

- opciones de canal y servidor;
- formateo/clasificación de errores RPC;
- creación, cierre y caché de canales;
- runtime específico de cliente `P2PStorage`;
- degradación adaptativa de batches cuando la respuesta supera el límite de mensaje.

`rpc/` se limita al transporte. Los clientes que conocen chunks, shards, metadata packs o eventos de membership viven cerca de su caso de uso.

### `cluster/`

Representa la frontera read-only entre clientes y membership:

- `membership_client.py` consulta un seed remoto;
- `resolver.py` decide si la vista de cluster es obligatoria u opcional;
- `view.py` expone `ClusterView` y `ClusterMember` como entrada estable para placement.

El protocolo SWIM sigue perteneciendo a `node/membership/`. `cluster/` solo obtiene una vista apta para flujos cliente.

### `placement/`

Contiene decisiones de placement puras: HRW / Rendezvous Hashing y cálculo de `placement_epoch`. Consume `ClusterView`, pero no consulta membership directamente.

### `cas/`

Es el Content Addressable Storage local. `cas/hashes.py` valida el significado de un hash como identidad de chunk; `common/hashes.py` solo valida la forma BLAKE3 genérica.

### `metadata/`

Es la fuente de verdad persistida y recuperable:

- `database.py`: persistencia SQLite;
- `identity/`: identidad y firmas de metadata;
- `objects/`: object graph cifrado/exportable;
- `packs/`: empaquetado, cifrado, firma, discovery, verificación de presencia y distribución de metadata packs;
- `crypto/`: primitivas KDF comunes del dominio metadata.

Metadata packs y EC data packs son subdominios distintos: comparten la palabra “pack”, pero no las mismas reglas de persistencia, cuotas ni recuperación.

Cada metadata DB tiene un `vault_id` estable. Los metadata packs guardan ese `vault_id` y una `vault_generation` que solo se compara dentro del mismo vault. Esto permite que un mismo owner publique varios vaults sin mezclar sus líneas de latest.

El ciclo remoto de metadata packs se divide en cuatro operaciones. `push` publica copias y guarda localmente la intención (`owner_id`, `pack_hash`, copias deseadas y resultados por target) en la DB de metadata. `discover` lista packs remotos del owner y calcula `presence_state` cuando conoce esa intención local; si no, muestra `UNKNOWN`. `verify` audita presencia remota mediante `ProbeMetadataPack` para un pack concreto o `ListMetadataPacks` para candidatos descubiertos. `recover` descarga e importa el pack elegido por `vault_id` + `vault_generation`, por `--vault-id` o por un `--target-hash` explícito.

`metadata pack verify` audita presencia del plano de control. No descarga packs ni recalcula el payload completo. Los packs están firmados y cifrados; cualquier manipulación se detecta en `recover`, que descarga, valida firma/hash y descifra el pack antes de importarlo. Esta separación evita usar una verificación rutinaria cara sobre metadata packs grandes.

La tabla `metadata_pack_publications` es estado operacional local: registra qué packs publicó esta instalación y con qué copias esperadas. No forma parte del metadata object graph ni se exporta dentro de metadata packs; en un equipo nuevo puede aparecer `desired_copies: unknown` hasta que vuelva a existir intención local.

### `backup/`

Caso de uso de creación de snapshots. Coordina scanning, chunking, CAS y metadata. Sus métricas de ejecución viven en `BackupRunStats`.

### `restore/`

Caso de uso de reconstrucción de snapshots. Coordina filesystem, prefetch, lectura de chunks, recuperación remota y EC. Sus métricas de ejecución viven en `RestoreRunStats`.

### `protection/`

Mantiene la protección remota registrada en metadata. Contiene dos estrategias hermanas:

- `protection/replication/`: chunks completos remotos;
- `protection/ec/`: data packs EC y shards remotos.

Los flujos de datos comparten scopes explícitos para `push` y `verify`. El scope por defecto es `pending`; `snapshot`, `all-reachable` y `all-known-chunks` acotan el conjunto de chunks desde metadata. Replication opera por chunks. EC empaqueta solo chunks aún no incluidos en data packs y reintenta packs existentes completos cuando el scope los alcanza.

Los verifiers trabajan por scope. EC también permite auditar un data pack concreto con `--pack-hash`.

### `node/`

Runtime servidor del nodo:

- `server.py`: composición y arranque;
- `services/`: adaptadores RPC inbound;
- `storage/`: commit engine y stores locales del nodo;
- `membership/`: protocolo SWIM, gossip y estado local;
- `identity.py`: identidad persistente del nodo.

## Fronteras RPC

Hay tres categorías distintas:

```text
rpc/                    mecánica de transporte
node/services/          adaptadores RPC inbound del servidor
*/remote*.py            clientes remotos outbound de cada caso de uso
```

No hay una capa gRPC de negocio común. Un cliente remoto de restore, uno de EC y uno de metadata packs pueden compartir canales y errores, pero no modelos ni excepciones de dominio.

## Pipeline de restore

Restore sigue este orden:

1. Se intenta CAS local principal.
2. Se intenta CAS P2P local del nodo, si existe.
3. Si `--remote-recovery replication|auto`, se consultan chunks completos por HRW.
4. Si `--remote-recovery ec|auto`, EC reconstruye únicamente chunks que siguen sin resolverse.
5. EC no compite con replication; actúa como ruta directa en modo `ec` o como fallback en modo `auto`.
6. `ChunkFetchService.fetch_many_raw_chunks()` devuelve `bytes` o `Exception` por chunk y no lanza por fallo individual.
7. `OrderedBatchChunkPrefetcher` convierte fallos individuales en aborto del archivo actual.
8. `SnapshotRestorer` no decide fuentes de datos; solo reconstruye rutas, archivos, directorios y metadata del filesystem.

## Métricas de runtime

Los flujos largos separan resultado y contadores:

```text
Result público: estado final consumido por CLI/tests.
RunStats: contadores acumulados del flujo.
Mutable stats privado: solo cuando la ejecución lo necesita internamente.
```

No hay una clase base común de stats. La homogeneidad es de convención, no de herencia.

Cuando un flujo produce un artefacto, el `Result` conserva la identidad del
artefacto y contiene un campo `stats` con sus contadores. Esto aplica a metadata
object graphs y metadata packs: rutas, hashes, generaciones y punteros viven en
el resultado; objetos leídos/escritos, bytes, targets, descargas e importaciones
viven en stats.

Los flujos que son principalmente mantenimiento o verificación pueden devolver
directamente stats, como `PushStats`, `ErasurePushStats`, `VerificationStats` o
`ErasureVerificationStats`.
