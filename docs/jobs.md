# Jobs periódicos

Stopan incluye un sistema sencillo de perfiles YAML para ejecutar tareas periódicas con systemd timers. El objetivo es facilitar que el administrador pueda escribir perfiles declarativos en `/etc/stopan/jobs/` y que el runner los convierta en variables de entorno para los scripts instalados en `/usr/lib/stopan/jobs/`.

## Ejecución

La unidad systemd genérica ejecuta:

```text
/usr/bin/flock -n /run/stopan/%i.lock /usr/lib/stopan/jobs/run-job %i
```

El lock evita solapamientos de la misma instancia. Si `stopan-job@backup-default.timer` dispara mientras `backup-default` sigue corriendo, el segundo arranque no se solapa.

El runner busca el perfil en:

```text
/etc/stopan/jobs/<nombre>.yaml
```

El perfil usado por `stopan-job@backup-default.service` es:

```text
/etc/stopan/jobs/backup-default.yaml
```

Activar un timer:

```bash
sudo systemctl enable --now stopan-job@backup-default.timer
```

Lanzar una ejecución manual:

```bash
sudo systemctl start stopan-job@backup-default.service
```

Ver logs:

```bash
journalctl -u stopan-job@backup-default.service
```

## Kinds

El campo `kind` es obligatorio. Los valores soportados por `stopan.jobs.runner` son:

```text
backup
data-push
data-restore
data-verify
metadata-graph
metadata-pack
gc
```

Cada kind solo admite su sección propia. Además, `backup`, `data-push` y `data-verify` pueden incluir una sección común `metadata` para activar auto-export de metadata object graph y auto-pack después de modificar metadata.

El perfil también puede tener una clave superior `config` para usar un YAML de configuración distinto de `/etc/stopan/stopan.yaml`.

## Validación

El runner rechaza perfiles vacíos, YAML no mapeado, `kind` ausente, kinds desconocidos, claves superiores no soportadas y claves no soportadas dentro de cada sección.

El nombre del perfil solo puede contener letras, números, `_`, `.`, `@` y `-`. Esto evita que una instancia systemd apunte fuera de `/etc/stopan/jobs`.

Los valores YAML simples se convierten a strings de entorno. Los booleanos se convierten a `true` o `false`. Los valores compuestos no están soportados.

## Backup

Ejemplo:

```yaml
kind: backup

backup:
  source: /srv/stopan/source
  workers: 4
  fast: true
  fast_remote: false
  deterministic: false
  desired_remote_copies: 3

metadata:
  object_graph: true
  object_pack: true
```

La sección `backup` se traduce al script `backup.sh`, que ejecuta `stopan backup`. La clave obligatoria es `source`.

Campos soportados:

```text
source
workers
fast
fast_remote
safe
deterministic
desired_remote_copies
membership_seed
```

La sección opcional `metadata` admite:

```text
object_graph
object_store
passphrase_file
identity_file
object_pack
object_pack_dir
```

## Data push

Replicación:

```yaml
kind: data-push

data_push:
  protection_mode: replication
  scope: pending
  remote_copies: 2
  strict_remote_copies: true
```

Erasure coding:

```yaml
kind: data-push

data_push:
  protection_mode: ec
  scope: pending
  ec_k: 2
  ec_m: 1
  ec_pack_size_bytes: 8388608
```

Campos soportados:

```text
protection_mode
scope
snapshot_id
limit
remote_copies
strict_remote_copies
ec_k
ec_m
ec_pack_size_bytes
target_parallelism
probe_batch_hashes
stream_inflight
probe_timeout_s
stream_timeout_s
commit_every
max_message_bytes
membership_seed
```

En `protection_mode: ec`, los campos de replicación no aplican.

## Data verify

Replicación:

```yaml
kind: data-verify

data_verify:
  protection_mode: replication
  scope: all-reachable
  reverify_verified: false
```

EC:

```yaml
kind: data-verify

data_verify:
  protection_mode: ec
  scope: all-reachable
  pack_hash:
```

Campos soportados:

```text
protection_mode
scope
snapshot_id
limit
reverify_verified
pack_hash
target_parallelism
probe_batch_hashes
probe_timeout_s
max_message_bytes
membership_seed
```

`pack_hash` solo tiene sentido con `protection_mode: ec`. `probe_batch_hashes` limita el tamaño de los lotes remotos tanto en replicación como en EC.

## Data restore

Ejemplo:

```yaml
kind: data-restore

data_restore:
  snapshot_id: 1
  out: /var/lib/stopan/restores
  remote_recovery: auto
  replication_targets: 2
```

Campos soportados:

```text
snapshot_id
out
remote_recovery
replication_targets
batch_target_parallelism
prefetch_window
membership_seed
```

`remote_recovery` acepta `none`, `replication`, `ec` y `auto`. Si se usa `none`, no debe pasarse `membership_seed`. Si se usa `ec`, `replication_targets` no aplica.

## Metadata graph

Exportar graph y crear pack:

```yaml
kind: metadata-graph

metadata_graph:
  action: export
  pack: true
  passphrase_file: /etc/stopan/metadata.passphrase
  object_store: /var/lib/stopan/metadata/object_store
  pack_dir: /var/lib/stopan/metadata/packs/generated
```

Importar graph:

```yaml
kind: metadata-graph

metadata_graph:
  action: import
  passphrase_file: /etc/stopan/metadata.passphrase
  object_store: /var/lib/stopan/metadata/object_store
  no_protection: false
```

Campos soportados:

```text
action
object_store
passphrase_file
identity_file
decrypt_latest
no_protection
pack
pack_out
pack_dir
default_desired_remote_copies
scrypt_n
scrypt_r
scrypt_p
key_length
```

El script ejecuta `stopan metadata graph <action>`.

## Metadata pack

Distribuir un pack:

```yaml
kind: metadata-pack

metadata_pack:
  action: push
  passphrase_file: /etc/stopan/metadata.passphrase
  pack_copies: 2
  strict_pack_copies: true
```

Recuperar metadata:

```yaml
kind: metadata-pack

metadata_pack:
  action: recover
  passphrase_file: /etc/stopan/metadata.passphrase
  owner_id:
  download_only: false
  no_import_db: false
  no_protection: false
```

Acciones soportadas por el CLI de metadata pack:

```text
create
inspect
list
import
push
discover
verify
recover
local-store
local-list
local-retrieve
```

Campos soportados por el perfil:

```text
action
path
object_store
pack_out
pack_dir
pack_in
passphrase_file
identity_file
owner_id
decrypt
expected_pack_hash
pack_store
pack_hash
local_retrieve_out
membership_seed
pack_copies
strict_pack_copies
target_parallelism
rpc_timeout_s
max_message_bytes
max_candidates
show_sources
verify_all
download_dir
download_only
download_pack_out
target_hash
no_import_db
no_protection
vault_id
scrypt_n
scrypt_r
scrypt_p
key_length
default_desired_remote_copies
```

## GC

Ejemplo de GC completo:

```yaml
kind: gc

gc:
  target: all
  apply: true
  passphrase_file: /etc/stopan/metadata.passphrase
```

Ejemplo de limpieza de packs recibidos:

```yaml
kind: gc

gc:
  target: received-metadata-packs
  apply: true
  max_age_days: 30
  pack_store: /var/lib/stopan/custody/metadata_packs
```

Campos soportados:

```text
target
apply
chunk_store
ec_store
grace_hours
max_age_days
object_store
passphrase_file
identity_file
object_grace_hours
pack_grace_hours
pack_dir
pack_store
```

Todos los GC son dry-run salvo que `apply` sea true.

## Ejemplos instalados

Los perfiles de ejemplo del árbol están en:

```text
packaging/examples/jobs/
```

En el paquete se instalan como documentación en:

```text
/usr/share/doc/stopan/examples/jobs/
```

El `postinst` no copia automáticamente esos perfiles a `/etc/stopan/jobs`. El administrador debe copiar solo los perfiles que quiera activar y adaptarlos a su entorno.
