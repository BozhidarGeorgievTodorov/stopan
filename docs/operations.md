# Operación de Stopan en Linux

Este documento describe una instalación orientada a administrador de sistemas. El
objetivo es ejecutar Stopan con rutas estables, usuario de sistema y tareas
periódicas mediante systemd timers.

## Layout recomendado

```text
/etc/stopan/
  node.yaml
  client.yaml
  node.env
  jobs/
    backup.sh
    backup.env
    push-replication.sh
    push-ec.sh
    metadata-export.sh
    metadata-pack-push.sh
    metadata-pack-push.env
    verify-replication.sh
    verify-ec.sh
    gc.sh
    gc.env

/opt/stopan/
  venv/

/var/lib/stopan/
  node_store/
  _data_chunks/
  _metadata.db
  metadata_object_store/
  metadata_distributed_packs/
  metadata_packs/

/var/log/stopan/
/run/stopan/
```

El paquete prepara el usuario `stopan`, los directorios base y las unidades
systemd. La aplicación Python se instala de forma autocontenida en
`/opt/stopan/venv`, y `/usr/bin/stopan` es un wrapper que ejecuta ese entorno.
Así las dependencias Python de Stopan no se mezclan con el Python global del
sistema.

En una instalación nueva también copia los YAML y jobs de ejemplo a
`/etc/stopan` si todavía no existen. En actualizaciones no los sobrescribe: ajusta
esos archivos a tu entorno.

## Servicio del nodo

El nodo persistente se ejecuta con:

```bash
sudo systemctl enable --now stopan-node.service
```

Por defecto usa:

```text
/etc/stopan/node.yaml
```

Puedes cambiar la ruta creando `/etc/stopan/node.env`:

```text
STOPAN_CONFIG=/etc/stopan/node.yaml
```

## Jobs periódicos

Las tareas periódicas usan una unidad genérica:

```text
stopan-job@.service
```

Cada instancia ejecuta:

```text
/etc/stopan/jobs/<nombre>.sh
```

con un lock local en:

```text
/run/stopan/<nombre>.lock
```

Eso evita solapamientos si una tarea anterior sigue en marcha cuando dispara el
siguiente timer.

Los ejemplos de scripts se instalan como documentación. Copia los que necesites:

```bash
sudo install -o stopan -g stopan -m 0750 /usr/share/doc/stopan/examples/jobs/backup.sh /etc/stopan/jobs/backup.sh
sudo install -o stopan -g stopan -m 0640 /usr/share/doc/stopan/examples/jobs/backup.env /etc/stopan/jobs/backup.env
```

Luego activa el timer correspondiente:

```bash
sudo systemctl enable --now stopan-job@backup.timer
```

## Tareas incluidas

### Backup

Script:

```text
/etc/stopan/jobs/backup.sh
```

Variables habituales:

```text
STOPAN_CONFIG=/etc/stopan/client.yaml
STOPAN_BACKUP_SOURCE=/srv/stopan/source
```

Timer de ejemplo:

```bash
sudo systemctl enable --now stopan-job@backup.timer
```

### Push por replicación

```bash
sudo install -o stopan -g stopan -m 0750 /usr/share/doc/stopan/examples/jobs/push-replication.sh /etc/stopan/jobs/push-replication.sh
sudo systemctl enable --now stopan-job@push-replication.timer
```

### Push EC

```bash
sudo install -o stopan -g stopan -m 0750 /usr/share/doc/stopan/examples/jobs/push-ec.sh /etc/stopan/jobs/push-ec.sh
sudo systemctl enable --now stopan-job@push-ec.timer
```

### Export de metadata graph

```bash
sudo install -o stopan -g stopan -m 0750 /usr/share/doc/stopan/examples/jobs/metadata-export.sh /etc/stopan/jobs/metadata-export.sh
sudo systemctl enable --now stopan-job@metadata-export.timer
```

### Push de metadata pack

El script puede publicar un pack concreto con `STOPAN_METADATA_PACK`. Si esa variable está vacía, usa el `.stopanmetapack` más reciente de `STOPAN_METADATA_PACK_DIR`.

```bash
sudo install -o stopan -g stopan -m 0750 /usr/share/doc/stopan/examples/jobs/metadata-pack-push.sh /etc/stopan/jobs/metadata-pack-push.sh
sudo install -o stopan -g stopan -m 0640 /usr/share/doc/stopan/examples/jobs/metadata-pack-push.env /etc/stopan/jobs/metadata-pack-push.env
sudo systemctl enable --now stopan-job@metadata-pack-push.timer
```

Variables habituales:

```text
STOPAN_METADATA_PACK=
STOPAN_METADATA_PACK_DIR=/var/lib/stopan/metadata_packs
```

### Verificación

```bash
sudo install -o stopan -g stopan -m 0750 /usr/share/doc/stopan/examples/jobs/verify-replication.sh /etc/stopan/jobs/verify-replication.sh
sudo systemctl enable --now stopan-job@verify-replication.timer
```

Para EC:

```bash
sudo install -o stopan -g stopan -m 0750 /usr/share/doc/stopan/examples/jobs/verify-ec.sh /etc/stopan/jobs/verify-ec.sh
sudo systemctl enable --now stopan-job@verify-ec.timer
```

### GC local

```bash
sudo install -o stopan -g stopan -m 0750 /usr/share/doc/stopan/examples/jobs/gc.sh /etc/stopan/jobs/gc.sh
sudo install -o stopan -g stopan -m 0640 /usr/share/doc/stopan/examples/jobs/gc.env /etc/stopan/jobs/gc.env
sudo systemctl enable --now stopan-job@gc.timer
```

Por defecto el script ejecuta:

```bash
stopan gc all --apply
```

Puedes cambiar el target con:

```text
STOPAN_GC_TARGET=received-metadata-packs
```

## Cambiar calendarios

Los timers incluidos son ejemplos. Para cambiar una periodicidad:

```bash
sudo systemctl edit stopan-job@backup.timer
```

Ejemplo:

```ini
[Timer]
OnCalendar=
OnCalendar=*-*-* 01:30:00
RandomizedDelaySec=15m
```

Después:

```bash
sudo systemctl daemon-reload
sudo systemctl restart stopan-job@backup.timer
```

## Ejecución manual y logs

Ejecutar una tarea manualmente:

```bash
sudo systemctl start stopan-job@backup.service
```

Ver logs:

```bash
journalctl -u stopan-node.service
journalctl -u stopan-job@backup.service
```

Listar timers:

```bash
systemctl list-timers 'stopan-*'
```

## Cron

También puedes usar cron llamando directamente a los scripts de `/etc/stopan/jobs`.
La ventaja de systemd timers es que integran logs, locks, estado de último run y
`Persistent=true` para disparar tareas pendientes tras un apagado.

## Validación operativa

Antes de construir el paquete o copiar las unidades a una máquina real, puedes ejecutar:

```bash
python3 e2e_operations_suite.py
```

Este smoke test valida el entrypoint `stopan`, los scripts de jobs, las unidades y timers systemd, el manifiesto Debian, las configuraciones de ejemplo y la documentación operativa.

## Construcción del paquete `.deb`

La carpeta `debian/` contiene una base de empaquetado con `debhelper`. El
paquete se construye como aplicación autocontenida: durante la build se crea un
entorno Python en `/opt/stopan/venv` y se instalan ahí Stopan y sus dependencias
Python desde `requirements.txt`. Desde la raíz del repositorio:

```bash
sudo apt install build-essential debhelper dpkg-dev python3-dev python3-pip python3-setuptools python3-venv python3-wheel
dpkg-buildpackage -us -uc -b
```

El paquete instala:

```text
/usr/bin/stopan
/opt/stopan/venv/
/lib/systemd/system/stopan-node.service
/lib/systemd/system/stopan-job@.service
/lib/systemd/system/stopan-job@*.timer
/usr/share/doc/stopan/examples/jobs/
/usr/share/doc/stopan/examples/configs/
```

El paquete final no depende de paquetes Debian como `python3-blake3` o
`python3-grpcio`: esas dependencias quedan dentro del entorno aislado de
`/opt/stopan/venv`. La construcción sí necesita acceso a las ruedas Python de
`requirements.txt`, ya sea mediante PyPI o mediante un wheelhouse interno.


## Validación de instalación real en Docker

Para comprobar que el paquete `.deb` se construye e instala correctamente en una
máquina limpia, puedes ejecutar:

```bash
python3 e2e_deb_install_suite.py
```

El test usa dos contenedores separados:

```text
builder
  instala herramientas de construcción y ejecuta dpkg-buildpackage

clean install
  instala solo el .deb generado y valida el comando stopan, el entorno
  autocontenido de /opt/stopan/venv, rutas /etc y /var, usuario de sistema,
  unidades systemd, jobs, timers y configs instaladas
```

Si quieres conservar el paquete generado:

```bash
python3 e2e_deb_install_suite.py --deb-output-dir dist-deb
```

También puedes cambiar las imágenes base:

```bash
python3 e2e_deb_install_suite.py   --builder-image debian:bookworm   --install-image debian:bookworm
```
