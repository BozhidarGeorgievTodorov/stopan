# Recuperación de metadata

Este documento cubre únicamente la protección y recuperación de metadata. La recuperación completa de una máquina perdida se describe en [`docs/disaster-recovery.md`](disaster-recovery.md).

La metadata es crítica para reconstruir un respaldo. Sin `_metadata.db` no basta con conservar los chunks en disco, porque faltan snapshots, rutas originales, recipes de reconstrucción y estado de protección.

Los ejemplos usan `stopan` directamente. En instalaciones donde `/etc/stopan` o `/var/lib/stopan` solo sean accesibles por un usuario de servicio, ejecuta los comandos con un usuario que tenga permisos suficientes.

## Qué se protege

Stopan no distribuye una copia binaria de `_metadata.db`. En su lugar, exporta la base de datos local a un metadata object graph cifrado. Ese graph contiene la información necesaria para reconstruir la metadata de snapshots, recetas, chunks y, si se incluye, estado de protección.

A partir del latest del object graph se puede crear un `.stopanmetapack`. El pack es un artefacto transportable, cifrado y firmado con la identidad de metadata del nodo propietario.

Flujo completo:

```text
_metadata.db
metadata object graph cifrado
.stopanmetapack cifrado y firmado
distribución remota
recover/import
_metadata.db reconstruida
```

Los nodos que almacenan metadata packs no necesitan descifrarlos. Solo guardan el artefacto y validan información como hash, firma, propietario, tamaño y cuotas.

## Identidad y secretos

La identidad de metadata se guarda normalmente en:

```text
/etc/stopan/metadata_identity.json
```

La passphrase se guarda normalmente en:

```text
/etc/stopan/metadata.passphrase
```

`owner_id` identifica al propietario de los metadata packs y permite descubrirlos en el clúster. No permite descifrarlos. Para una recuperación real hacen falta la identidad privada y la passphrase correcta.

La inicialización se hace una vez por nodo:

```bash
stopan init metadata
```

Este comando crea o valida la passphrase, crea o carga la identidad de metadata, escribe `metadata.owner_id` en `stopan.yaml` y muestra las claves públicas.

El administrador debe conservar de forma segura `metadata_identity.json` y la passphrase. Si se pierden, los metadata packs cifrados no podrán recuperarse aunque sigan disponibles en otros nodos.

## Flujo normal

La protección de metadata puede hacerse en tres pasos: exportar el object graph, crear un metadata pack y distribuirlo a otros nodos.

Exportar el estado actual de la base local:

```bash
stopan metadata graph export
```

Crear un metadata pack desde el latest del object graph:

```bash
stopan metadata pack create
```

Distribuir el pack a nodos remotos:

```bash
stopan metadata pack push
```

También se puede exportar y empaquetar en un solo paso:

```bash
stopan metadata graph export --pack
```

## Inspección local

Para ver el estado general de la configuración de metadata:

```bash
stopan metadata status
```

Para inspeccionar el metadata object store:

```bash
stopan metadata graph status
```

Para descifrar el latest del object graph y mostrar un resumen interno:

```bash
stopan metadata graph status --decrypt-latest
```

Para listar metadata packs locales sin descifrarlos:

```bash
stopan metadata pack list
```

Para inspeccionar un pack sin descifrar:

```bash
stopan metadata pack inspect pack.stopanmetapack
```

Para inspeccionarlo descifrando el contenido y validando su resumen:

```bash
stopan metadata pack inspect pack.stopanmetapack --decrypt
```

Para verificar íntegramente todos sus objetos sin importarlos:

```bash
stopan metadata pack inspect pack.stopanmetapack --decrypt --full-validation
```

## Distribución y verificación remota

Los comandos de distribución, discover, verify y recover necesitan contactar con el clúster. Normalmente usan los seeds definidos en `stopan.yaml`.

Distribuir el latest del object graph:

```bash
stopan metadata pack push
```

Distribuir un pack existente:

```bash
stopan metadata pack push --pack-in pack.stopanmetapack
```

`--pack-copies` controla cuántas copias remotas se buscan para el metadata pack. Esta política es independiente de `protection.remote_copies`, que aplica a chunks de datos.

Un valor `--pack-copies 0` crea o valida el pack local, pero no lo envía a otros nodos.

`--strict-pack-copies` exige suficientes nodos remotos elegibles antes de distribuir. Si no hay capacidad suficiente, el comando falla sin publicar una protección incompleta como si fuera válida.

Descubrir metadata packs remotos del owner configurado:

```bash
stopan metadata pack discover
```

Mostrar también los nodos donde aparece cada pack:

```bash
stopan metadata pack discover --show-sources
```

Verificar el latest descubierto:

```bash
stopan metadata pack verify
```

Verificar todos los candidatos descubiertos:

```bash
stopan metadata pack verify --all --show-sources
```

Verificar un pack concreto:

```bash
stopan metadata pack verify --pack-hash HASH
```

La verificación de metadata packs comprueba presencia remota. No reconstruye la base SQLite.

## Recuperar metadata desde packs remotos

Este procedimiento recupera la metadata desde packs remotos. La restauración posterior de datos se realiza con `restore` y se cubre en [`docs/disaster-recovery.md`](disaster-recovery.md).

En una máquina nueva o reparada, primero instala Stopan y prepara una configuración capaz de contactar con algún nodo vivo del clúster. También necesitas conservar la identidad de metadata y la passphrase.

Después restaura de forma segura:

```text
/etc/stopan/metadata_identity.json
/etc/stopan/metadata.passphrase
```

Recuperar metadata desde packs remotos:

```bash
stopan metadata pack recover
```

Durante la recuperación puede ser necesario pasar explícitamente el seed, la identidad o la passphrase si todavía no están disponibles en la configuración efectiva:

```bash
stopan metadata pack recover \
  --membership-seed node1.lan:50051 \
  --identity-file /etc/stopan/metadata_identity.json \
  --passphrase-file /etc/stopan/metadata.passphrase
```

Si hay packs de varios vaults bajo el mismo owner, usa `--vault-id` para elegir el vault correcto. Si conoces el pack exacto, usa `--target-hash`.

Descargar y validar íntegramente un pack, incluidos sus objetos, sin importarlo ni reconstruir la base:

```bash
stopan metadata pack recover \
  --download-only \
  --download-dir /var/lib/stopan/recovered_metadata
```

Importar el pack al object store local sin reconstruir `_metadata.db`:

```bash
stopan metadata pack recover --no-import-db
```

Reconstruir la base sin importar la protección previa:

```bash
stopan metadata pack recover \
  --no-protection \
  --default-desired-remote-copies 2
```

Esta última opción crea filas `PENDING` para los chunks conocidos. Es útil cuando se quiere reconstruir snapshots y recetas, pero no se desea confiar en el estado de protección incluido en el pack recuperado.

## Importación manual del graph

Si ya tienes un metadata object store recuperado localmente, puedes reconstruir `_metadata.db` desde su latest:

```bash
stopan metadata graph import
```

Para importar desde una ruta concreta:

```bash
stopan metadata graph import \
  --object-store /var/lib/stopan/metadata_object_store
```

Si el graph no incluye protection index, o si se quiere ignorarlo durante la importación:

```bash
stopan metadata graph import \
  --no-protection \
  --default-desired-remote-copies 2
```

`--default-desired-remote-copies` define cuántas copias remotas deseadas se asignan a las filas `PENDING` creadas durante la importación.

## Auto-export

`backup`, `push` y `verify` pueden exportar metadata automáticamente después de modificar la base SQLite local.

Flags comunes:

```text
--metadata-object-graph
--no-metadata-object-graph
--metadata-object-store DIR
--metadata-passphrase-file FILE
--metadata-identity-file FILE
--metadata-object-pack
--no-metadata-object-pack
--metadata-object-pack-dir DIR
```

Esto permite que cada backup o cambio de protección deje actualizado el object graph y, opcionalmente, genere un metadata pack.

Ejemplo:

```bash
stopan backup /srv/datos \
  --metadata-object-graph \
  --metadata-object-pack
```

Si `metadata.object_graph_auto_export` está activado en `stopan.yaml`, no hace falta pasar `--metadata-object-graph` en cada comando. Si `metadata.object_graph_auto_pack` también está activado, se genera un metadata pack después de exportar el graph.

## Opciones criptográficas avanzadas

Los comandos que cifran o descifran object graph y metadata packs pueden aceptar parámetros scrypt:

```text
--scrypt-n N
--scrypt-r N
--scrypt-p N
--metadata-key-length N
```

En uso normal no deberían cambiarse sin una razón clara. Si se modifican, conserva la configuración usada, porque será necesaria para descifrar artefactos antiguos.

## Comprobaciones periódicas

No basta con crear metadata packs. Conviene comprobar de forma periódica que se pueden distribuir, descubrir, verificar y descifrar.

Una comprobación razonable incluye:

```bash
stopan metadata graph export --pack
stopan metadata pack push
stopan metadata pack discover --show-sources
stopan metadata pack verify --all --show-sources
```

En una prueba controlada, también puede descargarse un pack sin importarlo:

```bash
stopan metadata pack recover \
  --download-only \
  --download-dir /var/lib/stopan/recovered_metadata
```

Después puede inspeccionarse el pack descargado con `metadata pack inspect --decrypt`.
