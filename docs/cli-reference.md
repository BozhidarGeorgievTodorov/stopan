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

## 2. Opciones avanzadas de Red y Protección

**Fast-path remoto durante el backup:**
Permite saltar chunks durante el backup si la metadata ya contiene evidencia suficiente de protección remota para el placement actual (no sube chunks a la red).

```bash
python -m stopan backup test_data 4 \
  --fast-remote \
  --membership-seed localhost:50051
```

**RF estricto y modo best-effort:**

Por defecto, `push` usa RF estricto. Si no hay suficientes candidatos remotos para cumplir las copias pedidas con `--rf`, el comando aborta antes de modificar `chunk_protection`.

```bash
python -m stopan push \
  --membership-seed localhost:50051 \
  --rf 4
```
Para permitir protección best-effort, se puede usar --no-strict-rf. En ese modo, push intenta colocar tantas copias como pueda y deja los chunks como DEGRADED si no alcanza las copias remotas pedidas.

```bash
  --membership-seed localhost:50051 \
  --rf 4 \
  --no-strict-rf
```

**Forzar re-verificación de chunks:**
Por defecto, el verifier salta chunks ya verificados. Para forzar una auditoría completa:

```bash
python -m stopan verify --membership-seed localhost:50051 --reverify-verified
```

## 3. Flujo manual de Metadata Distribuida

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

**Reconstruir la base SQLite a partir del object store importado:**

```bash
python -m stopan metadata import-graph \
  --object-store metadata_object_store_imported \
  --passphrase-file metadata.passphrase
```

## 4. Garbage Collection

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
