# Stopan: Manual de Operaciones y Referencia CLI

Este documento detalla los flujos avanzados, opciones de rendimiento y comandos manuales de bajo nivel para la administración del sistema Stopan. Para ver el flujo de inicio rápido y la arquitectura general, consulta el `README.md` principal.

## 1. Opciones avanzadas de Backup Local

Stopan permite alterar el comportamiento del recorrido y la generación de chunks durante el backup:

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


## 2. Matriz de semántica CLI para copias remotas

| Comando | Modo | Flags requeridos | Flags opcionales | Flags incompatibles | Significado |
|---|---|---|---|---|---|
| `backup` | snapshot local | `source_path` | `workers`, `--fast`, `--fast-remote`, `--safe`, `--deterministic`, `--desired-remote-copies` | `--safe` desactiva fast-path local y remoto | `--desired-remote-copies` solo guarda en metadata las copias remotas deseadas; no envía chunks. |
| `push` | `replication` | membership seed/config | `--remote-copies`, `--strict-remote-copies`, `--target-parallelism`, `--probe-batch-hashes`, `--stream-inflight` | `--ec-k`, `--ec-m`, `--ec-pack-size-bytes` | `--remote-copies` son copias remotas completas por chunk; la copia local no cuenta. |
| `push` | `ec` | membership seed/config, `--ec-k`, `--ec-m` | `--ec-pack-size-bytes`, `--limit`, timeouts | `--remote-copies`, `--strict-remote-copies`, flags de probe/stream de replicación | No usa copias remotas completas; usa `ec_k + ec_m` shards remotos. |
| `verify` | `replication` | membership seed/config | `--target-parallelism`, `--probe-batch-hashes`, `--reverify-verified` | flags EC de `push` | No acepta flag de copias; lee de metadata las copias remotas deseadas de cada chunk. |
| `verify` | `ec` | membership seed/config | `--target-parallelism`, `--reverify-verified` | `--probe-batch-hashes`, flags EC de `push` | No usa copias remotas completas ni `--ec-k/--ec-m`; lee specs EC desde metadata. |
| `restore` | `none` | `snapshot_id` | `--out`, `--prefetch-window` | `--membership-seed`, `--replication-targets` | No usa red. Es el default. |
| `restore` | `replication`/`auto` | `snapshot_id`, membership seed/config | `--replication-targets`, `--batch-target-parallelism`, `--prefetch-window` | flags EC de `push` | `--replication-targets` limita cuántos targets HRW se consultan por chunk ausente; no es factor de protección. |
| `restore` | `ec` | `snapshot_id`, membership seed/config | `--batch-target-parallelism`, `--prefetch-window` | `--replication-targets`, flags EC de `push` | Reconstruye desde data packs EC registrados; no usa targets de replicación. |
| `metadata push` | pack distribuido | identity/passphrase, membership seed/config | `--pack-copies`, `--strict-pack-copies`, `--target-parallelism` | `--pack-in` con `--object-store`, `--pack-out` o `--pack-dir`; `--pack-out` con `--pack-dir` | `--pack-copies` son copias remotas del metadata pack, separadas de la política de chunks. |
| `metadata recover` / `metadata import-graph` | reconstrucción metadata | según comando | `--default-desired-remote-copies` | flags de `push`/EC | Solo rellena metadata `PENDING` si no se importa `chunk_protection`. |

## 3. Opciones avanzadas de Red y Protección

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

El tamaño objetivo máximo del payload de cada data pack se puede ajustar con `--ec-pack-size-bytes`:

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

## 4. Flujo manual de Metadata Distribuida

Aunque `backup` puede actualizar y empaquetar el grafo automáticamente, el flujo se puede descomponer en pasos manuales para auditoría o recuperación granular:

**Exportar el grafo de metadata desde SQLite a un object store local:**

```bash
python -m stopan metadata export-graph \
  --object-store metadata_object_store \
  --passphrase-file metadata.passphrase \
  --identity-file metadata_identity.json
```

**Empaquetar un grafo previamente exportado:**

```bash
python -m stopan metadata pack-graph \
  --object-store metadata_object_store \
  --passphrase-file metadata.passphrase \
  --identity-file metadata_identity.json \
  --out latest.stopanmetapack
```

**Inspeccionar el contenido de un pack (metadatos públicos):**

```bash
python -m stopan metadata inspect-pack latest.stopanmetapack
```

**Inspeccionar descifrando su contenido interno:**

```bash
python -m stopan metadata inspect-pack latest.stopanmetapack \
  --decrypt \
  --identity-file metadata_identity.json \
  --passphrase-file metadata.passphrase
```

**Importar un pack externo a un object store local:**

```bash
python -m stopan metadata import-pack latest.stopanmetapack \
  --object-store metadata_object_store_imported \
  --passphrase-file metadata.passphrase \
  --identity-file metadata_identity.json
```

**Distribuir un metadata pack por la red P2P:**

```bash
python -m stopan metadata push \
  --object-store metadata_object_store \
  --passphrase-file metadata.passphrase \
  --identity-file metadata_identity.json \
  --membership-seed localhost:50051 \
  --pack-copies 3
```

**Reconstruir la base SQLite a partir del object store importado:**

```bash
python -m stopan metadata import-graph \
  --object-store metadata_object_store_imported \
  --passphrase-file metadata.passphrase
```

## 5. Garbage Collection

Stopan permite limpiar objetos y packs de metadata que ya no forman parte del estado vivo.

**Limpieza de objetos sueltos exclusivamente (modo seguro/dry-run):**

```bash
python -m stopan metadata gc \
  --object-store metadata_object_store \
  --passphrase-file metadata.passphrase \
  --identity-file metadata_identity.json \
  --objects-only \
  --dry-run
```

**Limpieza de packs exclusivamente (modo seguro/dry-run):**

```bash
python -m stopan metadata gc \
  --object-store metadata_object_store \
  --passphrase-file metadata.passphrase \
  --identity-file metadata_identity.json \
  --packs-only \
  --dry-run
```

**Aplicar los borrados de forma permanente:**
Sustituir `--dry-run` por `--apply` en cualquiera de los comandos anteriores. Para el store distribuido P2P:

```bash
python -m stopan metadata-store-gc \
  --config configs/node1.yaml \
  --apply
```
