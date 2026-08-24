# Despliegue en red local
Este documento describe cómo montar un clúster básico de Stopan en una LAN usando el paquete `.deb`, `/etc/stopan/stopan.yaml` y `stopan-node.service`.

## Modelo de red

Cada máquina ejecuta un proceso `stopan node`. Cada nodo necesita:

- `node.bind_addr`, dirección local de escucha
- `node.advertise_addr`, dirección alcanzable por otros nodos
- `cluster.token`, común al clúster lógico
- `cluster.seeds`, puntos de entrada para descubrir miembros
- rutas persistentes para datos, metadata y packs

El token separa clústeres lógicos. No sustituye a TLS, VPN, firewall ni aislamiento de red. En una LAN real, el puerto gRPC debe estar accesible solo para las máquinas del clúster.

## Configuración base

El paquete instala un ejemplo de configuración principal en:

```text
/usr/share/doc/stopan/examples/configs/stopan.yaml
```

Durante `postinst`, si todavía no existe `/etc/stopan/stopan.yaml`, se copia una configuración base a esa ruta. Después, cada máquina debe personalizar al menos `node.advertise_addr`, `cluster.token` y `cluster.seeds`.

También puede generarse un ejemplo con `stopan config example`.

Antes de arrancar el nodo, valida el fichero:

```bash
sudo stopan config validate /etc/stopan/stopan.yaml
```

## Inicializar nodos

La forma recomendada de preparar una máquina es usar `stopan init node`, porque actualiza el YAML de forma controlada y crea los directorios principales. El comando exige un `--token` no vacío y `stopan node` rechaza el arranque si la credencial no está configurada.

Nodo 1:

```bash
sudo stopan init node \
  --advertise-addr node1.lan:50051 \
  --bind-addr '[::]:50051' \
  --token oficina \
  --seed node1.lan:50051 \
```

Nodo 2:

```bash
sudo stopan init node \
  --advertise-addr node2.lan:50051 \
  --bind-addr '[::]:50051' \
  --token oficina \
  --seed node1.lan:50051 \
```

Nodo 3:

```bash
sudo stopan init node \
  --advertise-addr node3.lan:50051 \
  --bind-addr '[::]:50051' \
  --token oficina \
  --seed node1.lan:50051 \
  --seed node2.lan:50051 \
```

`advertise_addr` debe ser resoluble y alcanzable desde el resto de máquinas. Puede usarse DNS local o IP fija, por ejemplo `192.168.1.10:50051`.

Los seeds externos son puntos de entrada, no miembros fijados permanentemente. Si están temporalmente inaccesibles durante el arranque, el nodo continúa funcionando y vuelve a intentar la incorporación con una espera creciente, acotada y desincronizada mediante jitter hasta descubrir un par. El primer nodo puede arrancar aislado porque `stopan init node` usa su propio `advertise_addr` como seed cuando no se proporciona ninguno y ese contacto propio se descarta durante el bootstrap.

`stopan init node` no inicializa la identidad de metadata. Solo configura nodo, clúster y política básica de protección de datos.

## Inicializar metadata

`stopan init node` no inicializa la identidad de metadata. En los nodos que vayan a crear backups o metadata packs, ejecuta:

```bash
sudo stopan init metadata
```

Este comando usa `metadata.passphrase_file` y `metadata.identity_file`, crea la identidad si falta, calcula `owner_id` y actualiza el YAML.

Guarda fuera de la máquina origen estos secretos:

```text
/etc/stopan/metadata.passphrase
/etc/stopan/metadata_identity.json
```

Sin esos secretos, `owner_id` permite descubrir packs, pero no descifrarlos ni reconstruir la metadata.

## Arrancar servicios

Arranca el nodo en cada máquina:

```bash
sudo systemctl enable --now stopan-node.service
```

Comprobaciones:

```bash
stopan node status
stopan node status --address node2.lan:50051
sudo systemctl status stopan-node.service
sudo journalctl -u stopan-node.service
sudo stopan config validate /etc/stopan/stopan.yaml
```

`stopan node` falla de forma controlada si `node.advertise_addr` está vacío.

Para una parada ordenada puede usarse cualquiera de estas rutas:

```bash
sudo stopan node stop
sudo systemctl stop stopan-node.service
```

En ambos casos el nodo deja de admitir trabajo nuevo, anuncia `LEFT` a la vista de membresía y espera el trabajo ya iniciado antes de terminar. La unidad systemd no impone un timeout de parada porque el drenaje preserva las operaciones en curso.

## Capacidad mínima

Para replicación completa, cada origen necesita tantos nodos remotos elegibles como indique `remote_copies`. La copia local no cuenta.

Con tres nodos totales y `remote_copies: 2`, cada origen puede colocar dos copias remotas, una en cada una de las otras máquinas. Con dos nodos totales, `remote_copies: 2` no cabe.

Para EC, cada origen necesita `ec_k + ec_m` nodos remotos elegibles. Con `ec_k: 2` y `ec_m: 1`, hacen falta tres remotos por origen. Como el origen se excluye, eso implica al menos cuatro nodos totales.

## Prueba de humo

Una vez arrancados los nodos, ejecuta una prueba mínima en la máquina que contiene los datos:

```bash
stopan backup /srv/datos
stopan push
stopan verify
```

Ese flujo crea primero un snapshot local y después protege los chunks en nodos remotos según la configuración.

Para comprobar la metadata distribuida:

```bash
stopan metadata graph export --pack
stopan metadata pack push
stopan metadata pack verify --all
```

## Checklist

Antes de considerar estable un clúster local, conviene comprobar:

- el paquete está instalado en todos los nodos
- cada nodo tiene `node.advertise_addr` único y alcanzable
- todos los nodos comparten `cluster.token`
- todos los nodos tienen seeds válidos
- el puerto gRPC solo está expuesto a máquinas del clúster
- `stopan config validate` pasa en cada máquina
- `stopan-node.service` está activo en cada máquina
- `stopan node status` responde localmente
- un nodo puede consultar a otro con `--address`
- la capacidad de nodos cumple la política de replication o EC configurada
- los secretos de metadata están guardados fuera del nodo origen
