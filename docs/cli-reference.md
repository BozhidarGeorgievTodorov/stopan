# Stopan: Manual de Operaciones y Referencia CLI

Este documento detalla los flujos avanzados, opciones de rendimiento y comandos manuales de bajo nivel para la administración del sistema Stopan. Para ver el flujo de inicio rápido consulta el `README.md`; para arquitectura interna y responsabilidades de carpetas consultar `docs/architecture.md`.

## 1. Opciones avanzadas de Backup Local

Stopan permite alterar el comportamiento del recorrido y la generación de chunks durante el backup. Si no se pasa el argumento posicional `workers`, se usa `backup.workers` del YAML:

**Procesamiento multihilo:**

```bash
python -m stopan backup test_data 4
```

**Recorrido determinista (ideal para pruebas y validación de hashes):**

```bash
python -m stopan backup test_data --deterministic
```

**Activar fast-path local (reutiliza chunks presentes en el CAS y evita escrituras/trabajo innecesario):**

```bash
python -m stopan backup test_data --fast
```

**Forzar modo seguro (procesa todo ignorando saltos rápidos):**

```bash
python -m stopan backup test_data --safe
```


## 2. Matriz de flags para copias remotas

| Comando | Modo | Flags requeridos | Flags opcionales | Flags incompatibles | Significado |
|---|---|---|---|---|---|
| `backup` | snapshot local | `source_path` | `workers`, `--fast`, `--fast-remote`, `--safe`, `--deterministic`, `--desired-remote-copies` | `--safe` desactiva fast-path local y remoto | `--desired-remote-copies` solo guarda en metadata las copias remotas deseadas; no envía chunks. |
| `push` | `replication` | membership seed/config | `--remote-copies`, `--strict-remote-copies`, `--target-parallelism`, `--probe-batch-hashes`, `--stream-inflight`, `--scope`, `--snapshot-id` | `--ec-k`, `--ec-m`, `--ec-pack-size-bytes` | `--remote-copies` son copias remotas completas por chunk; la copia local no cuenta. Puede acotarse por snapshot o scope. |
| `push` | `ec` | membership seed/config, `--ec-k`, `--ec-m` | `--ec-pack-size-bytes`, `--limit`, `--scope`, `--snapshot-id`, timeouts | `--remote-copies`, `--strict-remote-copies`, flags de probe/stream de replicación | No usa copias remotas completas; usa `ec_k + ec_m` shards remotos. Puede acotarse por snapshot o scope sin duplicar chunks ya empaquetados. |
| `verify` | `replication` | membership seed/config | `--target-parallelism`, `--probe-batch-hashes`, `--reverify-verified`, `--scope`, `--snapshot-id` | flags EC de `push`, `--pack-hash` | No acepta flag de copias; lee de metadata las copias remotas deseadas de cada chunk y puede acotar la auditoría por snapshot o scope. |
| `verify` | `ec` | membership seed/config | `--target-parallelism`, `--reverify-verified`, `--scope`, `--snapshot-id`, `--pack-hash` | `--probe-batch-hashes`, flags EC de `push`; `--pack-hash` con `--scope`/`--snapshot-id` | No usa copias remotas completas ni `--ec-k/--ec-m`; lee specs EC desde metadata y puede auditar un data pack concreto. |
| `restore` | `none` | `snapshot_id` | `--out`, `--prefetch-window` | `--membership-seed`, `--replication-targets` | No usa red. Es el default. |
| `restore` | `replication`/`auto` | `snapshot_id`, membership seed/config | `--replication-targets`, `--batch-target-parallelism`, `--prefetch-window` | flags EC de `push` | `--replication-targets` limita cuántos targets HRW se consultan por chunk ausente; no es factor de protección. |
| `restore` | `ec` | `snapshot_id`, membership seed/config | `--batch-target-parallelism`, `--prefetch-window` | `--replication-targets`, flags EC de `push` | Reconstruye desde data packs EC registrados; no usa targets de replicación. |
| `metadata pack push` | pack distribuido | identity/passphrase, membership seed/config | `--pack-in`, `--object-store`, `--pack-out`, `--pack-dir`, `--pack-copies`, `--strict-pack-copies`, `--target-parallelism` | `--pack-in` con `--object-store`, `--pack-out` o `--pack-dir`; `--pack-out` con `--pack-dir` | `--pack-copies` son copias remotas del metadata pack, separadas de la política de chunks. |
| `metadata pack discover` | inventario remoto | owner/identity, membership seed/config | `--max-candidates`, `--show-sources` | flags de descarga/importación | Lista packs remotos del owner. Muestra `presence_state` solo si existe publicación local con copias esperadas; si no, `UNKNOWN`. |
| `metadata pack verify` | auditoría remota | owner/identity, membership seed/config | `--pack-hash`, `--all`, `--max-candidates`, `--show-sources` | `--pack-hash` con `--all` | Verifica presencia remota contra publicaciones locales persistidas. Con `--pack-hash` usa `ProbeMetadataPack`; sin hash usa discovery. Si no conoce la intención, muestra `UNKNOWN`. |
| `metadata pack recover` / `metadata graph import` | reconstrucción metadata | según comando | `--target-hash`, `--vault-id`, `--download-only`, `--no-import-db`, `--default-desired-remote-copies` | flags de `push`/EC | `recover` elige el latest válido si no hay selector; puede limitarse a descargar el pack, dejarlo preparado en el object store o reconstruir la DB. |

## 3. Contrato general de flags

Los flags largos se escriben completos. Stopan rechaza prefijos implícitos para mantener estable el contrato público del CLI.

Las validaciones de rango se hacen antes de iniciar trabajo real:

- valores `>= 0`: copias deseadas que permiten modo local-only, `ec_m`, edades de GC y periodos de gracia.
- valores `>= 1`: workers, `backup.workers`, `snapshot_id`, límites, paralelismos, tamaños de mensaje, ventanas de restore, `ec_k`, `protection.ec_pack_size_bytes`, `commit_every`, `max_candidates`, `metadata.pack_discovery_max_candidates`, `metadata.pack_target_parallelism`, `metadata.cli_warning_limit`, `metadata pack push --pack-copies` y parámetros scrypt `r/p`.
- valores `> 0`: timeouts RPC/probe/stream y `metadata.pack_rpc_timeout_s`.
- `--scrypt-n` debe ser `>= 2` y potencia de dos; `--metadata-key-length` debe ser `>= 32`.


Los límites operativos de descubrimiento de metadata packs siguen la configuración efectiva:

- `metadata.pack_discovery_max_candidates` define el límite por defecto para `metadata pack discover`, `metadata pack verify` sin `--pack-hash` y `metadata pack recover` automático.
- `metadata.pack_target_parallelism` y `metadata.pack_rpc_timeout_s` definen los defaults de paralelismo y timeout de RPC para push/discover/verify/recover de metadata packs.
- `metadata.cli_warning_limit` define cuántos warnings/errores repetitivos imprime la CLI de metadata antes de resumir el resto.
- `--max-candidates` puede sobrescribir el límite de candidatos en una ejecución concreta.
- `metadata pack discover` y `metadata pack verify` usan la publicación local persistida por `metadata pack push` para conocer las copias esperadas. Si no existe publicación local, imprimen `desired_copies: unknown` y `presence_state: UNKNOWN`.
- `metadata pack verify` comprueba presencia distribuida. `metadata pack recover` descarga el pack elegido y valida firma/hash antes de importarlo. Si el owner tiene packs de varios vaults, la recuperación automática requiere `--vault-id` o `--target-hash`.

También se rechazan combinaciones que dejarían flags ignorados:

- `metadata graph status --passphrase-file` requiere `--decrypt-latest`.
- `metadata graph export --pack-out`, `--pack-dir` o `--identity-file` requieren `--pack`; `--pack-out` y `--pack-dir` son incompatibles.
- `metadata pack create --out` y `--pack-dir` son incompatibles.
- `metadata pack inspect --passphrase-file` o `--identity-file` requieren `--decrypt`.
- `metadata pack verify --pack-hash` y `--all` son incompatibles.
- `metadata pack recover --pack-out` y `--download-dir` son incompatibles; `--target-hash` limita la recuperación a un pack concreto y `--vault-id` limita la selección automática a un vault.
- `metadata pack recover --download-only` es incompatible con `--no-import-db`, `--no-protection` y `--default-desired-remote-copies`.
- `metadata pack recover --no-import-db` es incompatible con `--no-protection` y con `--default-desired-remote-copies`.

Se mantienen tres precedencias explícitas: `backup --safe` desactiva fast-path aunque se pase `--fast` o `--fast-remote`; `metadata pack push --pack-copies 0` puede combinarse con `--membership-seed` aunque no envíe el pack; y `metadata pack list --pack-dir` tiene prioridad sobre `--object-store`.

Los errores controlados del CLI se muestran sin traceback por defecto y usan prefijos estables según categoría:

- `Error de uso de Stopan`: flags, combinaciones inválidas o configuración efectiva incompatible con el comando.
- `Error de configuración de Stopan`: fichero YAML ausente, inválido o insuficiente.
- `Error de datos de Stopan`: metadata, packs, hashes o estado persistido inconsistente.
- `Error de almacenamiento de Stopan`: rutas, repositorios o stores locales no accesibles.
- `Error de red de Stopan`: membership, RPC, peers o streams remotos no completados correctamente.
- `Error de dependencias de Stopan`: dependencias opcionales necesarias para el flujo solicitado.
- `Error de Stopan`: fallo interno no clasificado como error controlado de usuario.

Para depuración, `STOPAN_DEBUG=1` conserva el traceback original.

## 4. Opciones avanzadas de Red y Protección

**Fast-path remoto durante el backup:**
Permite saltar chunks durante el backup si la metadata ya contiene evidencia suficiente de protección remota por replicación para el placement actual (no sube chunks a la red). El objetivo de protección que se guarda en metadata se puede fijar con `--desired-remote-copies`.

```bash
python -m stopan backup test_data \
  --fast-remote \
  --membership-seed localhost:50051 \
  --desired-remote-copies 3
```

**Protección por replicación de chunks:**

Por defecto, `push` usa `--protection-mode replication`. En este modo, `--remote-copies` indica cuántas copias remotas completas se requieren por chunk. La copia local no cuenta y el nodo origen se excluye del placement.

`push` acepta scopes de protección. El scope por defecto es `pending`: procesa chunks/data packs pendientes, degradados, fallidos o con `placement_epoch` obsoleto según metadata. `--scope snapshot --snapshot-id ID` limita el trabajo a chunks alcanzables desde un snapshot completo; `--scope all-reachable` usa todos los chunks alcanzables desde snapshots completos; `--scope all-known-chunks` usa todos los chunks registrados en metadata.

```bash
python -m stopan push \
  --membership-seed localhost:50051 \
  --remote-copies 3
```

**Copias estrictas y modo best-effort:**

La configuración por defecto usa modo best-effort (`protection.strict_remote_copies: false`). En ese modo, `push` intenta colocar tantas copias como pueda y deja los chunks como `DEGRADED` si no alcanza las copias remotas pedidas.

Para exigir capacidad completa antes de modificar `chunk_protection`, usa `--strict-remote-copies`. Si la configuración activa el modo estricto, se puede desactivar para una ejecución con `--no-strict-remote-copies`.

```bash
python -m stopan push \
  --membership-seed localhost:50051 \
  --remote-copies 3 \
  --strict-remote-copies
```

**Protección por erasure coding:**

El modo EC agrupa chunks deduplicados en data packs, codifica cada pack con `ec_k` data shards y `ec_m` shards extra de redundancia, y guarda cada shard en un nodo remoto distinto. En el clúster Docker de 4 nodos, el nodo origen queda excluido y los 3 nodos remotos permiten usar `ec_k=2`, `ec_m=1`.

`ec_m=0` está permitido. En ese caso Stopan hace striping sin redundancia: genera `ec_k` shards, necesita todos para reconstruir y cualquier shard perdido deja el pack en `FAILED`.

```bash
python -m stopan push \
  --membership-seed localhost:50051 \
  --protection-mode ec \
  --ec-k 2 \
  --ec-m 1
```

El tamaño objetivo máximo del payload de cada data pack se toma de `protection.ec_pack_size_bytes` y se puede sobrescribir con `--ec-pack-size-bytes`:

```bash
python -m stopan push \
  --membership-seed localhost:50051 \
  --protection-mode ec \
  --ec-k 2 \
  --ec-m 1 \
  --ec-pack-size-bytes 8388608
```

Semántica de estados EC:

```text
VERIFIED  -> están presentes todos los shards del pack
DEGRADED  -> faltan shards, pero quedan al menos ec_k shards recuperables; con ec_m=0 no hay estado degradado útil
FAILED    -> quedan menos de ec_k shards recuperables
```

**Forzar re-verificación de chunks replicados:**
Por defecto, el verifier salta chunks ya verificados. Para forzar una auditoría completa del modo de replicación:

```bash
python -m stopan verify --membership-seed localhost:50051 --reverify-verified
```

**Verificar data packs EC:**

```bash
python -m stopan verify --protection-mode ec
```

Para verificar un data pack EC concreto:

```bash
python -m stopan verify --protection-mode ec --pack-hash HASH
```

Para acotar la auditoría de datos por snapshot o scope:

```bash
python -m stopan verify --scope snapshot --snapshot-id 1
python -m stopan verify --scope all-reachable
python -m stopan verify --protection-mode ec --scope all-known-chunks
```

Para volver a auditar packs EC ya verificados:

```bash
python -m stopan verify --protection-mode ec --reverify-verified
```

**Restore y selección explícita de recuperación remota:**

`restore` siempre prioriza CAS local y store P2P local. La recuperación remota está desactivada por defecto (`--remote-recovery none`) y se activa de forma explícita con `--remote-recovery`:

```text
none         -> no usa red; solo stores locales
replication  -> recupera chunks completos desde nodos P2P; usa --membership-seed y puede limitarse con --replication-targets
ec           -> reconstruye desde data packs EC registrados; no usa targets de replicación
auto         -> intenta replication y usa EC como fallback para lo que siga faltando
```

Recuperación por replicación de chunks:

```bash
python -m stopan restore 1 \
  --out restored_from_replication \
  --remote-recovery replication \
  --membership-seed localhost:50051 \
  --replication-targets 3
```

Recuperación desde data packs EC:

```bash
python -m stopan restore 1 \
  --out restored_from_ec \
  --remote-recovery ec \
  --membership-seed localhost:50051
```

Ruta combinada con fallback explícito:

```bash
python -m stopan restore 1 \
  --out restored_auto \
  --remote-recovery auto \
  --membership-seed localhost:50051 \
  --replication-targets 3
```

## 5. Flujo manual de Metadata Distribuida

Aunque `backup` puede actualizar y empaquetar el grafo automáticamente, el flujo se puede descomponer en pasos manuales para auditoría o recuperación granular.

El metadata object graph usa `--passphrase-file`. Los metadata packs usan `--passphrase-file` y `--identity-file`. `metadata graph export` y `metadata graph import` no necesitan identidad; `metadata pack create`, `metadata pack import`, `metadata pack push`, `metadata pack recover`, `metadata pack inspect --decrypt` y `gc generated-metadata-packs` sí la usan cuando trabajan con packs.

**Exportar el grafo de metadata desde SQLite a un object store local:**

```bash
python -m stopan metadata graph export \
  --object-store metadata_object_store \
  --passphrase-file metadata.passphrase
```

**Empaquetar un grafo previamente exportado:**

```bash
python -m stopan metadata pack create \
  --object-store metadata_object_store \
  --passphrase-file metadata.passphrase \
  --identity-file metadata_identity.json \
  --out latest.stopanmetapack
```

**Inspeccionar el contenido de un pack (metadatos públicos):**

```bash
python -m stopan metadata pack inspect latest.stopanmetapack
```

**Inspeccionar descifrando su contenido interno:**

```bash
python -m stopan metadata pack inspect latest.stopanmetapack \
  --decrypt \
  --identity-file metadata_identity.json \
  --passphrase-file metadata.passphrase
```

**Importar un pack externo a un object store local:**

```bash
python -m stopan metadata pack import latest.stopanmetapack \
  --object-store metadata_object_store_imported \
  --passphrase-file metadata.passphrase \
  --identity-file metadata_identity.json
```

**Distribuir un metadata pack por la red P2P:**

```bash
python -m stopan metadata pack push \
  --object-store metadata_object_store \
  --passphrase-file metadata.passphrase \
  --identity-file metadata_identity.json \
  --membership-seed localhost:50051 \
  --pack-copies 3
```

**Descubrir y verificar metadata packs distribuidos del owner:**

```bash
python -m stopan metadata pack discover \
  --identity-file metadata_identity.json \
  --membership-seed localhost:50051 \
  --show-sources

python -m stopan metadata pack verify \
  --identity-file metadata_identity.json \
  --membership-seed localhost:50051 \
  --all
```

**Recuperar un metadata pack distribuido concreto:**

```bash
python -m stopan metadata pack recover \
  --object-store metadata_object_store_imported \
  --passphrase-file metadata.passphrase \
  --identity-file metadata_identity.json \
  --membership-seed localhost:50051 \
  --vault-id VAULT_ID

python -m stopan metadata pack recover \
  --object-store metadata_object_store_imported \
  --passphrase-file metadata.passphrase \
  --identity-file metadata_identity.json \
  --membership-seed localhost:50051 \
  --target-hash PACK_HASH
```

**Reconstruir la base SQLite a partir del object store importado:**

```bash
python -m stopan metadata graph import \
  --object-store metadata_object_store_imported \
  --passphrase-file metadata.passphrase
```

## 6. Garbage Collection

Stopan centraliza el garbage collection local en `python -m stopan gc ...`. No hay comunicación con otros nodos: cada target limpia únicamente los directorios locales configurados o indicados por CLI.

**Limpieza de objetos del metadata graph generado localmente (modo seguro/dry-run):**

```bash
python -m stopan gc generated-metadata-graph \
  --object-store metadata_object_store \
  --passphrase-file metadata.passphrase \
  --identity-file metadata_identity.json \
  --dry-run
```

**Limpieza de metadata packs generados localmente:**

```bash
python -m stopan gc generated-metadata-packs \
  --object-store metadata_object_store \
  --passphrase-file metadata.passphrase \
  --identity-file metadata_identity.json \
  --dry-run
```

**Limpieza del store distribuido de metadata packs recibidos:**

```bash
python -m stopan gc received-metadata-packs \
  --config configs/node1.yaml \
  --dry-run
```

**Aplicar los borrados de forma permanente:**
Sustituir `--dry-run` por `--apply` en cualquiera de los comandos anteriores.
