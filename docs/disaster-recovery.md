# Recuperación ante pérdida de máquina

Este documento describe el procedimiento completo para recuperar la base de datos de una máquina perdida o reemplazada. Cubre la preparación del nuevo nodo, la recuperación de metadata, la restauración de datos y las comprobaciones posteriores.

No explica en detalle el formato de metadata object graph ni de `.stopanmetapack`. Ese dominio está en [`docs/metadata-recovery.md`](metadata-recovery.md).

## Alcance

Este procedimiento aplica cuando se pierde la máquina que creó snapshots, pero siguen existiendo otros nodos del clúster con datos y metadata packs suficientes.

El objetivo es reconstruir una máquina operativa que pueda:

- contactar con el clúster
- recuperar `_metadata.db`
- conocer snapshots, recipes y chunks
- restaurar datos desde CAS local recuperado, réplicas remotas o shards EC
- volver a publicar metadata y protección tras la recuperación

## Requisitos previos

Para una recuperación completa se necesita:

- un paquete Stopan instalable en la máquina nueva
- al menos un nodo vivo del clúster accesible por red
- configuración suficiente para conocer `cluster.token` y seeds
- `metadata_identity.json` del owner original
- passphrase asociada a esa identidad
- metadata packs distribuidos accesibles en otros nodos
- chunks protegidos por replication o EC según la política usada

`owner_id` por sí solo no permite recuperar metadata. Sirve para identificar y descubrir packs, pero no para descifrarlos.

Si se han perdido la identidad de metadata o la passphrase, los metadata packs cifrados no podrán descifrarse aunque sigan existiendo en otros nodos.

Si nunca se ejecutó `push`, o si la protección remota estaba degradada antes del fallo, la metadata puede recuperarse pero algunos datos pueden no estar disponibles.

## Orden general

El orden recomendado es:

```text
instalar Stopan
inicializar configuración de nodo
restaurar secretos de metadata
validar identidad de metadata
arrancar el servicio de nodo
recuperar metadata desde packs remotos
restaurar snapshots
verificar protección
exportar y distribuir metadata actualizada
```

## Preparar la máquina nueva

Instala el paquete Stopan:

```bash
sudo apt install ./stopan_1.0.0_amd64.deb
```

Inicializa la configuración del nodo. Usa una dirección anunciada alcanzable por el resto del clúster y seeds que apunten a nodos vivos:

```bash
sudo stopan init node \
  --advertise-addr node-new.lan:50051 \
  --bind-addr '[::]:50051' \
  --token oficina \
  --seed node1.lan:50051 \
  --seed node2.lan:50051
```

Si la máquina nueva sustituye a una anterior, no tiene por qué reutilizar la misma `advertise_addr`. Lo importante es que el nuevo valor sea alcanzable y que el clúster tenga al menos un seed vivo.

Valida la configuración:

```bash
sudo stopan config validate /etc/stopan/stopan.yaml
```

## Restaurar secretos de metadata

Antes de ejecutar `stopan init metadata`, restaura los secretos conservados fuera de la máquina perdida:

```text
metadata.passphrase
metadata_identity.json
```

Instálalos en las rutas esperadas por `stopan.yaml`:

```bash
sudo install -o root -g stopan -m 0640 metadata.passphrase /etc/stopan/metadata.passphrase
sudo install -o root -g stopan -m 0640 metadata_identity.json /etc/stopan/metadata_identity.json
```

Antes de continuar, comprueba que `metadata.passphrase_file` y `metadata.identity_file` en `stopan.yaml` apuntan a esos ficheros restaurados.

Después ejecuta:

```bash
sudo stopan init metadata
```

Si las rutas son correctas, el comando leerá la passphrase desde el fichero, validará que desbloquea la identidad restaurada y actualizará `metadata.owner_id` en `stopan.yaml`.

Si `metadata.identity_file` apunta a una ruta inexistente, `init metadata` puede crear una identidad nueva. Esa identidad no servirá para descifrar packs antiguos.

Comprueba el estado de metadata:

```bash
stopan metadata status
stopan metadata identity-show
```

## Arrancar y comprobar el nodo

Arranca el servicio:

```bash
sudo systemctl enable --now stopan-node.service
```

Comprueba el servicio y la conectividad básica:

```bash
sudo systemctl status stopan-node.service
stopan node status
stopan node status --address node1.lan:50051
```

Si el nodo no arranca, revisa primero la configuración efectiva:

```bash
sudo stopan config validate /etc/stopan/stopan.yaml
sudo journalctl -u stopan-node.service
```

## Recuperar metadata

La recuperación de metadata descarga un pack remoto válido, lo importa al object store local y reconstruye `_metadata.db`.

Primero comprueba qué packs son visibles:

```bash
stopan metadata pack discover --show-sources
stopan metadata pack verify --all --show-sources
```

Después recupera la metadata:

```bash
stopan metadata pack recover
```

Los casos de recuperación manual con parámetros explícitos, selección por vault, descarga sin importación o reconstrucción sin protección previa están documentados en [`docs/metadata-recovery.md`](metadata-recovery.md).

## Comprobar la metadata recuperada

Después de recuperar metadata, comprueba que Stopan ve el estado local:

```bash
stopan metadata status
stopan metadata graph status
```

## Restaurar datos

Una vez reconstruida `_metadata.db`, Stopan vuelve a conocer snapshots, recipes y hashes de chunks. A partir de ahí, restaura el snapshot que quieras recuperar.

```bash
stopan restore <SNAPSHOT_ID>
```

## Verificar y volver a publicar protección

Cuando la metadata y los datos estén recuperados, verifica la protección remota:

```bash
stopan verify --scope all-reachable
```

Si el resultado muestra degradación, revisa capacidad del clúster, seeds, token y nodos vivos. Después ejecuta `push` para reintentar protección:

```bash
stopan push --scope all-reachable
stopan verify --scope all-reachable
```

Actualiza la protección de metadata:

```bash
stopan metadata graph export --pack
stopan metadata pack push
stopan metadata pack verify --all --show-sources
```

Este paso es importante porque la máquina recuperada puede tener una nueva configuración local, un nuevo estado de metadata y nuevos resultados de verificación.

## Casos especiales

### Solo se perdió `_metadata.db`

Si el nodo sigue existiendo y conserva configuración, identidad, passphrase y stores locales, no hace falta reconstruir toda la máquina. Recupera metadata con:

```bash
stopan metadata pack recover
```

O importa un object graph local ya recuperado:

```bash
stopan metadata graph import
```

### Se recuperó metadata pero faltan datos

Esto indica que los snapshots y recipes existen, pero no todos los chunks necesarios están disponibles local o remotamente.

Comprueba:

- si se ejecutó `push` antes del fallo
- si la política era replication o EC
- si hay nodos remotos vivos suficientes
- si `verify` mostraba degradación antes del fallo
- si la recuperación remota puede consultar los seeds configurados

### La identidad restaurada no descifra packs

No ejecutes una recuperación destructiva. Comprueba que estás usando la passphrase correcta y el `metadata_identity.json` del owner original.

`owner_id` debe coincidir con la identidad esperada, pero no sustituye a la identidad privada ni a la passphrase.

### Se quiere ignorar el estado de protección recuperado

Puedes reconstruir metadata sin confiar en la protección previa:

```bash
stopan metadata pack recover \
  --no-protection \
  --default-desired-remote-copies 2
```

Esto crea filas `PENDING` para chunks conocidos. Después ejecuta `push` y `verify` para recomponer protección.

## Checklist final

Antes de dar por cerrada la recuperación, comprueba:

- Stopan está instalado en la máquina nueva
- `/etc/stopan/stopan.yaml` valida correctamente
- `node.advertise_addr` es alcanzable desde otros nodos
- el nodo puede contactar con al menos un seed vivo
- `metadata.passphrase` e `metadata_identity.json` son los originales
- `stopan init metadata` no ha creado una identidad nueva
- `metadata pack discover` ve packs remotos
- `metadata pack recover` reconstruye `_metadata.db`
- `restore --remote-recovery auto` completa el snapshot esperado
- `verify` no muestra degradación inesperada
- `metadata graph export --pack` funciona
- `metadata pack push` y `metadata pack verify --all` confirman protección de metadata

## Límites

Este procedimiento no puede recuperar datos que nunca fueron protegidos remotamente ni metadata packs que nunca fueron distribuidos.

Tampoco sustituye a copias externas de secretos. La identidad de metadata y la passphrase deben guardarse fuera del nodo origen antes del desastre.
