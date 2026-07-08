from __future__ import annotations

import argparse
import hashlib
import os
import re
import shutil
import sqlite3
import subprocess
import tempfile
import textwrap
import time
import random
from dataclasses import dataclass
from pathlib import Path, PurePosixPath


CONTAINER_ROOT = PurePosixPath("/e2e")
DEFAULT_IMAGE_TAG = "stopan:e2e-acceptance"
DEFAULT_NETWORK_PREFIX = "stopan-e2e-acceptance"
BASE_CLUSTER_TOKEN = "stopan-e2e-acceptance"
PASSPHRASE = "stopan-e2e-acceptance-passphrase"
DEFAULT_DATASET_MB = 128


class E2EError(RuntimeError):
    pass


@dataclass(frozen=True)
class Scenario:
    name: str
    protection_mode: str
    node_count: int
    remote_copies: int
    metadata_pack_copies: int
    ec_k: int = 0
    ec_m: int = 0

    @property
    def aliases(self) -> tuple[str, ...]:
        return tuple(f"node{index}" for index in range(1, self.node_count + 1))

    @property
    def origin_addr(self) -> str:
        return self.addr("node1")

    @property
    def cluster_token(self) -> str:
        return f"{BASE_CLUSTER_TOKEN}-{self.name}"

    def addr(self, alias: str) -> str:
        return f"{alias}:50051"

    def all_addrs(self) -> list[str]:
        return [self.addr(alias) for alias in self.aliases]

    def peer_addrs(self, alias: str) -> list[str]:
        return [self.addr(peer) for peer in self.aliases if peer != alias]

    def validate(self) -> None:
        if self.protection_mode not in {"replication", "ec"}:
            raise E2EError(f"Modo de protección no soportado: {self.protection_mode}")
        if self.node_count < 2:
            raise E2EError("El E2E necesita al menos dos nodos")
        if self.remote_copies < 1:
            raise E2EError("remote_copies debe ser >= 1")
        if self.metadata_pack_copies < 1:
            raise E2EError("metadata_pack_copies debe ser >= 1")

        remote_nodes = self.node_count - 1
        if remote_nodes < self.remote_copies:
            raise E2EError(
                f"{self.name} necesita {self.remote_copies} nodos remotos, pero solo hay {remote_nodes}"
            )
        if remote_nodes < self.metadata_pack_copies:
            raise E2EError(
                f"{self.name} necesita {self.metadata_pack_copies} nodos remotos para metadata packs, "
                f"pero solo hay {remote_nodes}"
            )
        if self.protection_mode == "ec":
            total_shards = self.ec_k + self.ec_m
            if self.ec_k < 1 or self.ec_m < 0:
                raise E2EError("EC requiere ec_k >= 1 y ec_m >= 0")
            if remote_nodes < total_shards:
                raise E2EError(
                    f"EC necesita {total_shards} nodos remotos para k={self.ec_k}, m={self.ec_m}, "
                    f"pero solo hay {remote_nodes}"
                )


def find_repo_root(start: Path) -> Path:
    for candidate in (start, *start.parents):
        if (candidate / "Dockerfile").is_file() and (candidate / "pyproject.toml").is_file():
            return candidate
    raise E2EError("No se encontró la raíz del repositorio.")


ROOT = find_repo_root(Path(__file__).resolve().parent)


def docker_user_args() -> list[str]:
    if os.name == "posix":
        return ["--user", f"{os.getuid()}:{os.getgid()}"]
    return []


def run_cmd(
    args: list[str],
    *,
    cwd: Path | None = None,
    timeout: float = 120.0,
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        args,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
    )
    if completed.returncode != 0:
        raise E2EError(
            "Comando falló\n"
            f"cmd: {' '.join(args)}\n"
            f"rc: {completed.returncode}\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}\n"
        )
    return completed


def docker(args: list[str], *, timeout: float = 120.0) -> str:
    return run_cmd(["docker", *args], cwd=ROOT, timeout=timeout).stdout


def docker_ok(args: list[str], *, timeout: float = 120.0) -> bool:
    try:
        completed = subprocess.run(
            ["docker", *args],
            cwd=ROOT,
            text=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return False
    return completed.returncode == 0


def network_name(prefix: str, scenario: Scenario) -> str:
    return f"{prefix}-{scenario.name}"


def container_name(scenario: Scenario, alias: str) -> str:
    return f"stopan-e2e-{scenario.name}-{alias}"


def build_runtime_image(image_tag: str) -> None:
    print(f"Construyendo imagen runtime Stopan desde .deb: {image_tag}")
    docker(["build", "--target", "runtime", "-t", image_tag, "."], timeout=1200.0)


def ensure_network(name: str) -> None:
    if not docker_ok(["network", "inspect", name], timeout=30.0):
        docker(["network", "create", name], timeout=60.0)


def cleanup_scenario_docker(scenario: Scenario, network_prefix: str) -> None:
    for alias in scenario.aliases:
        docker_ok(["rm", "-f", container_name(scenario, alias)], timeout=30.0)
    docker_ok(["network", "rm", network_name(network_prefix, scenario)], timeout=30.0)


def container_path(tmp: Path, path: Path) -> str:
    rel = path.relative_to(tmp).as_posix()
    return str(CONTAINER_ROOT / rel)


def host_path_from_container(tmp: Path, path: Path) -> Path:
    raw = path.as_posix()
    prefix = str(CONTAINER_ROOT) + "/"
    if not raw.startswith(prefix):
        raise E2EError(f"Ruta fuera del volumen E2E: {raw}")
    return tmp / raw[len(prefix):]


def docker_stopan(
    tmp: Path,
    *,
    image_tag: str,
    network: str,
    args: list[str],
    timeout: float = 120.0,
) -> str:
    cmd = [
        "run",
        "--rm",
        "--network",
        network,
        *docker_user_args(),
        "-v",
        f"{tmp.as_posix()}:{CONTAINER_ROOT.as_posix()}",
        image_tag,
        "stopan",
        *args,
    ]
    output = docker(cmd, timeout=timeout)
    if output:
        print(output, end="" if output.endswith("\n") else "\n")
    return output


def start_node(
    tmp: Path,
    *,
    image_tag: str,
    network: str,
    scenario: Scenario,
    alias: str,
    config_path: str,
) -> None:
    name = container_name(scenario, alias)
    docker_ok(["rm", "-f", name], timeout=30.0)
    docker(
        [
            "run",
            "-d",
            "--name",
            name,
            "--network",
            network,
            "--network-alias",
            alias,
            *docker_user_args(),
            "-v",
            f"{tmp.as_posix()}:{CONTAINER_ROOT.as_posix()}",
            image_tag,
            "stopan",
            "node",
            "--config",
            config_path,
        ],
        timeout=60.0,
    )


def node_logs(scenario: Scenario, alias: str) -> str:
    try:
        return docker(["logs", container_name(scenario, alias)], timeout=30.0)
    except Exception as exc:
        return f"No se pudieron leer logs del nodo {alias}: {exc}"


def wait_node_ready(scenario: Scenario, alias: str, *, timeout: float = 35.0) -> None:
    name = container_name(scenario, alias)
    deadline = time.monotonic() + timeout
    last_output = ""

    while time.monotonic() < deadline:
        running = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", name],
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if running.returncode != 0 or running.stdout.strip() != "true":
            raise E2EError(f"El contenedor {alias} no está en ejecución.\nLogs:\n{node_logs(scenario, alias)}")

        probe = subprocess.run(
            [
                "docker",
                "exec",
                name,
                "stopan",
                "node",
                "status",
                "--config",
                f"{CONTAINER_ROOT.as_posix()}/configs/{alias}.yaml",
                "--address",
                "127.0.0.1:50051",
            ],
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        last_output = probe.stdout + probe.stderr
        if probe.returncode == 0 and "node_serving: yes" in probe.stdout:
            return
        time.sleep(0.5)

    raise E2EError(
        f"El nodo {alias} no quedó listo.\nÚltima salida:\n{last_output}\nLogs:\n{node_logs(scenario, alias)}"
    )


def wait_cluster_members(
    tmp: Path,
    *,
    image_tag: str,
    network: str,
    scenario: Scenario,
    client_config: str,
    timeout: float = 60.0,
) -> None:
    deadline = time.monotonic() + timeout
    expected = scenario.node_count
    last_output = ""

    while time.monotonic() < deadline:
        try:
            output = docker_stopan(
                tmp,
                image_tag=image_tag,
                network=network,
                args=["node", "status", "--config", client_config, "--address", scenario.origin_addr],
                timeout=20.0,
            )
            last_output = output
            match = re.search(r"members_alive:\s*(\d+)", output)
            if match and int(match.group(1)) >= expected:
                return
        except Exception as exc:
            last_output = str(exc)
        time.sleep(0.75)

    logs = "\n".join(f"[{alias}]\n{node_logs(scenario, alias)}" for alias in scenario.aliases)
    raise E2EError(
        f"El cluster no alcanzó {expected} miembros vivos.\nÚltima salida:\n{last_output}\nLogs:\n{logs}"
    )


def write_text_file(path: Path, content: str, *, mode: int | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    if mode is not None:
        path.chmod(mode)


def write_stream_file(path: Path, size: int, *, seed: int, mode: int = 0o644) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    rng = random.Random(seed)
    remaining = size
    block_size = 1024 * 1024

    with path.open("wb") as fh:
        while remaining > 0:
            current = min(block_size, remaining)
            fh.write(rng.randbytes(current))
            remaining -= current

    path.chmod(mode)


def copy_file(src: Path, dst: Path, *, mode: int = 0o644) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, dst)
    dst.chmod(mode)


def create_source_tree(source_dir: Path, *, dataset_mb: int) -> None:

    print(f"Generando dataset determinista de aceptación: {dataset_mb} MiB aprox.")
    source_dir.mkdir(parents=True, exist_ok=True)
    (source_dir / "vacio" / "subdir_sin_archivos").mkdir(parents=True, exist_ok=True)

    write_text_file(source_dir / "README.txt", "Stopan E2E de aceptación\n", mode=0o644)
    write_text_file(source_dir / "documentos" / "nota con espacios.txt", "archivo con espacios\n", mode=0o644)
    write_text_file(source_dir / "documentos" / "unicode_ñandú_🧪.txt", "ñandú, backup, restore, metadata\n", mode=0o644)
    write_text_file(source_dir / "documentos" / "empty.txt", "", mode=0o644)
    write_text_file(source_dir / "config" / "app.yaml", "nombre: stopan\nmodo: e2e\n", mode=0o644)
    write_text_file(source_dir / "web" / "index.html", "<html><body>Stopan E2E</body></html>\n", mode=0o644)

    for index in range(120):
        write_text_file(
            source_dir / "muchos-pequenos" / f"item-{index:03d}.json",
            f'{{"index": {index}, "texto": "contenido pequeño repetible", "grupo": {index % 7}}}\n',
            mode=0o644,
        )

    total_bytes = dataset_mb * 1024 * 1024
    fixed_small = 6 * 1024 * 1024
    large_budget = max(total_bytes - fixed_small, 16 * 1024 * 1024)

    large_a = max(8 * 1024 * 1024, large_budget * 40 // 100)
    large_b = max(8 * 1024 * 1024, large_budget * 25 // 100)
    medium_a = max(4 * 1024 * 1024, large_budget * 12 // 100)
    medium_b = max(2 * 1024 * 1024, large_budget * 8 // 100)
    repeated = max(2 * 1024 * 1024, large_budget * 5 // 100)

    write_stream_file(source_dir / "grandes" / "imagen-disco-A.bin", large_a, seed=11)
    write_stream_file(source_dir / "grandes" / "archivo-video-B.raw", large_b, seed=29)
    write_stream_file(source_dir / "medianos" / "dump.sqlite.bin", medium_a, seed=43)
    write_stream_file(source_dir / "medianos" / "dataset.csv.bin", medium_b, seed=71)

    repeated_src = source_dir / "deduplicacion" / "bloque-base.bin"
    write_stream_file(repeated_src, repeated, seed=97)
    copy_file(repeated_src, source_dir / "deduplicacion" / "copia-1.bin")
    copy_file(repeated_src, source_dir / "deduplicacion" / "copia-2.bin")
    copy_file(repeated_src, source_dir / "deduplicacion" / "subdir" / "copia-3.bin")

    write_stream_file(source_dir / "bordes" / "un-byte.bin", 1, seed=3)
    write_stream_file(source_dir / "bordes" / "casi-1MiB.bin", 1024 * 1024 - 17, seed=5)
    write_stream_file(source_dir / "bordes" / "algo-mas-1MiB.bin", 1024 * 1024 + 19, seed=7)


def yaml_list(values: list[str], indent: int = 14) -> str:
    prefix = " " * indent
    return "\n".join(f'{prefix}- "{value}"' for value in values)


def write_node_config(path: Path, *, scenario: Scenario, alias: str) -> None:
    repo_dir = CONTAINER_ROOT / "nodes" / alias / "repo"
    state_dir = CONTAINER_ROOT / "nodes" / alias / "state"

    write_text_file(
        path,
        textwrap.dedent(
            f"""
            node:
              bind_addr: "[::]:50051"
              advertise_addr: "{scenario.addr(alias)}"
              repo_store_dir: "{repo_dir}"
              local_shard_dir: "{state_dir / '_data_chunks'}"
              db_file: "{state_dir / '_metadata.db'}"

            cluster:
              token: "{scenario.cluster_token}"
              seeds:
{yaml_list(scenario.peer_addrs(alias), indent=16)}

            protection:
              remote_copies: {scenario.remote_copies}
              ec_k: {scenario.ec_k or 2}
              ec_m: {scenario.ec_m or 1}

            metadata:
              distributed_pack_store_dir: "{repo_dir / 'metadata_packs'}"
            """
        ).lstrip(),
    )


def write_client_config(path: Path, *, scenario: Scenario) -> None:
    client_dir = CONTAINER_ROOT / "client"

    write_text_file(
        path,
        textwrap.dedent(
            f"""
            node:
              bind_addr: "127.0.0.1:0"
              advertise_addr: "{scenario.origin_addr}"
              repo_store_dir: "{client_dir / 'repo'}"
              local_shard_dir: "{client_dir / '_data_chunks'}"
              db_file: "{client_dir / '_metadata.db'}"

            cluster:
              token: "{scenario.cluster_token}"
              seeds:
{yaml_list(scenario.all_addrs(), indent=16)}

            protection:
              remote_copies: {scenario.remote_copies}
              ec_k: {scenario.ec_k or 2}
              ec_m: {scenario.ec_m or 1}

            metadata:
              passphrase_file: "{client_dir / 'metadata.passphrase'}"
              identity_file: "{client_dir / 'metadata_identity.json'}"
              object_store_dir: "{client_dir / 'metadata_object_store'}"
              object_pack_dir: "{client_dir / 'packs'}"
              distributed_pack_store_dir: "{client_dir / 'local_metadata_pack_store'}"
            """
        ).lstrip(),
    )


def remove_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def remove_sqlite_family(db_path: Path) -> None:
    for suffix in ("", "-wal", "-shm"):
        remove_path(Path(str(db_path) + suffix))


def snapshot_id_from_backup(output: str) -> int:
    match = re.search(r"Backup completado:\s+snapshot\s+(\d+)", output)
    if not match:
        raise E2EError(f"No se pudo extraer snapshot_id del backup:\n{output}")
    return int(match.group(1))


def final_restore_dir(output: str) -> Path:
    match = re.search(r"Directorio final:\s*(.+)", output)
    if not match:
        raise E2EError(f"No se pudo extraer el directorio final del restore:\n{output}")
    return Path(match.group(1).strip())


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def tree_manifest(root: Path) -> dict[str, tuple[str, int | None, str | None]]:
    manifest: dict[str, tuple[str, int | None, str | None]] = {}
    for path in sorted(root.rglob("*")):
        rel = path.relative_to(root).as_posix()
        if path.is_dir():
            manifest[rel] = ("dir", None, None)
        elif path.is_file():
            manifest[rel] = ("file", path.stat().st_size, file_sha256(path))
        else:
            manifest[rel] = ("other", None, None)
    return manifest


def assert_same_tree(expected: Path, actual: Path) -> None:
    expected_manifest = tree_manifest(expected)
    actual_manifest = tree_manifest(actual)
    problems: list[str] = []

    expected_keys = set(expected_manifest)
    actual_keys = set(actual_manifest)
    for rel in sorted(expected_keys - actual_keys):
        problems.append(f"falta en restore: {rel}")
    for rel in sorted(actual_keys - expected_keys):
        problems.append(f"sobra en restore: {rel}")
    for rel in sorted(expected_keys & actual_keys):
        if expected_manifest[rel] != actual_manifest[rel]:
            problems.append(f"contenido o tipo distinto: {rel}")

    if problems:
        raise E2EError("El árbol restaurado no coincide:\n" + "\n".join(problems))


def sqlite_scalar(db_path: Path, sql: str) -> int:
    if not db_path.is_file():
        raise E2EError(f"No existe la DB de metadata esperada: {db_path}")

    with sqlite3.connect(db_path) as conn:
        try:
            row = conn.execute(sql).fetchone()
        except sqlite3.OperationalError as exc:
            tables = [
                table_row[0]
                for table_row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
                ).fetchall()
            ]
            raise E2EError(
                f"SQL inválida contra la DB de metadata: {sql}\n"
                f"Tablas disponibles: {', '.join(tables)}"
            ) from exc

    return int(row[0] if row else 0)


def assert_metadata_has_backup(db_path: Path, *, context: str) -> None:
    snapshots = sqlite_scalar(
        db_path,
        "SELECT COUNT(*) FROM snapshots WHERE status = 'COMPLETE'",
    )
    items = sqlite_scalar(db_path, "SELECT COUNT(*) FROM snapshot_items")
    files = sqlite_scalar(
        db_path,
        "SELECT COUNT(*) FROM snapshot_items WHERE item_type = 'file'",
    )
    dirs = sqlite_scalar(
        db_path,
        "SELECT COUNT(*) FROM snapshot_items WHERE item_type = 'dir'",
    )
    chunks = sqlite_scalar(db_path, "SELECT COUNT(*) FROM chunks")

    print(
        f"Metadata ({context}): "
        f"snapshots={snapshots} items={items} files={files} dirs={dirs} chunks={chunks}"
    )

    if snapshots <= 0 or items <= 0 or files <= 0 or dirs <= 0 or chunks <= 0:
        raise E2EError(f"Metadata incompleta durante {context}")


def assert_ec_metadata_complete(db_path: Path, *, context: str) -> None:
    chunks = sqlite_scalar(db_path, "SELECT COUNT(*) FROM chunks")
    packs = sqlite_scalar(db_path, "SELECT COUNT(*) FROM erasure_data_packs")
    pack_chunks = sqlite_scalar(db_path, "SELECT COUNT(*) FROM erasure_data_pack_chunks")
    pack_shards = sqlite_scalar(db_path, "SELECT COUNT(*) FROM erasure_data_pack_shards")
    unpacked_chunks = sqlite_scalar(
        db_path,
        """
        SELECT COUNT(*)
        FROM chunks c
        LEFT JOIN erasure_data_pack_chunks epc ON epc.chunk_hash = c.hash
        WHERE epc.chunk_hash IS NULL
        """,
    )
    print(
        f"Metadata EC ({context}): chunks={chunks} packs={packs} "
        f"pack_chunks={pack_chunks} shards={pack_shards} unpacked_chunks={unpacked_chunks}"
    )
    if chunks <= 0 or packs <= 0 or pack_chunks <= 0 or pack_shards <= 0 or unpacked_chunks != 0:
        raise E2EError(f"Metadata EC incompleta durante {context}")


def init_metadata(tmp: Path, *, image_tag: str, network: str, client_config: str) -> None:
    print("Inicializando identidad de metadata...")
    docker_stopan(
        tmp,
        image_tag=image_tag,
        network=network,
        args=["init", "metadata", "--config", client_config],
        timeout=180.0,
    )


def create_backup(
    tmp: Path,
    *,
    image_tag: str,
    network: str,
    scenario: Scenario,
    client_config: str,
) -> int:
    print("Creando backup local...")
    output = docker_stopan(
        tmp,
        image_tag=image_tag,
        network=network,
        args=[
            "backup",
            "--config",
            client_config,
            str(CONTAINER_ROOT / "source"),
            "2",
            "--deterministic",
            "--safe",
            "--desired-remote-copies",
            str(scenario.remote_copies),
        ],
        timeout=900.0,
    )
    snapshot_id = snapshot_id_from_backup(output)
    print(f"Snapshot creado: {snapshot_id}")
    return snapshot_id


def push_chunks(tmp: Path, *, image_tag: str, network: str, scenario: Scenario, client_config: str) -> None:
    if scenario.protection_mode == "replication":
        print(f"Subiendo chunks por replication con remote_copies={scenario.remote_copies}...")
        docker_stopan(
            tmp,
            image_tag=image_tag,
            network=network,
            args=[
                "push",
                "--config",
                client_config,
                "--protection-mode",
                "replication",
                "--remote-copies",
                str(scenario.remote_copies),
            ],
            timeout=1200.0,
        )
        return

    print(f"Subiendo chunks por EC k={scenario.ec_k}, m={scenario.ec_m}...")
    docker_stopan(
        tmp,
        image_tag=image_tag,
        network=network,
        args=[
            "push",
            "--config",
            client_config,
            "--protection-mode",
            "ec",
            "--ec-k",
            str(scenario.ec_k),
            "--ec-m",
            str(scenario.ec_m),
        ],
        timeout=1200.0,
    )


def verify_protection(tmp: Path, *, image_tag: str, network: str, scenario: Scenario, client_config: str) -> None:
    print(f"Verificando protección remota por {scenario.protection_mode}...")
    args = [
        "verify",
        "--config",
        client_config,
        "--protection-mode",
        scenario.protection_mode,
        "--reverify-verified",
    ]
    docker_stopan(tmp, image_tag=image_tag, network=network, args=args, timeout=900.0)


def export_and_push_metadata_pack(
    tmp: Path,
    *,
    image_tag: str,
    network: str,
    scenario: Scenario,
    client_config: str,
) -> None:
    print("Exportando metadata graph, creando pack y distribuyéndolo...")
    pack_path = CONTAINER_ROOT / "client" / "packs" / f"{scenario.name}.stopanmetapack"
    docker_stopan(
        tmp,
        image_tag=image_tag,
        network=network,
        args=[
            "metadata",
            "graph",
            "export",
            "--config",
            client_config,
            "--object-store",
            str(CONTAINER_ROOT / "client" / "metadata_object_store"),
            "--passphrase-file",
            str(CONTAINER_ROOT / "client" / "metadata.passphrase"),
            "--identity-file",
            str(CONTAINER_ROOT / "client" / "metadata_identity.json"),
            "--pack",
            "--pack-out",
            str(pack_path),
        ],
        timeout=600.0,
    )
    docker_stopan(
        tmp,
        image_tag=image_tag,
        network=network,
        args=[
            "metadata",
            "pack",
            "push",
            "--config",
            client_config,
            "--pack-in",
            str(pack_path),
            "--passphrase-file",
            str(CONTAINER_ROOT / "client" / "metadata.passphrase"),
            "--identity-file",
            str(CONTAINER_ROOT / "client" / "metadata_identity.json"),
            "--pack-copies",
            str(scenario.metadata_pack_copies),
        ],
        timeout=600.0,
    )


def verify_metadata_pack_distribution(
    tmp: Path,
    *,
    image_tag: str,
    network: str,
    client_config: str,
) -> None:
    print("Verificando metadata packs distribuidos...")
    docker_stopan(
        tmp,
        image_tag=image_tag,
        network=network,
        args=[
            "metadata",
            "pack",
            "verify",
            "--config",
            client_config,
            "--all",
            "--show-sources",
        ],
        timeout=600.0,
    )


def simulate_client_disaster(client_dir: Path, restore_base: Path) -> None:
    print("Simulando desastre local: se conservan solo identidad y passphrase de metadata...")
    remove_sqlite_family(client_dir / "_metadata.db")
    remove_path(client_dir / "_data_chunks")
    remove_path(client_dir / "repo")
    remove_path(client_dir / "metadata_object_store")
    remove_path(client_dir / "packs")
    remove_path(client_dir / "local_metadata_pack_store")
    remove_path(client_dir / "recovered_object_store")
    remove_path(client_dir / "recovered.stopanmetapack")
    remove_path(restore_base)


def recover_metadata(
    tmp: Path,
    *,
    image_tag: str,
    network: str,
    client_config: str,
) -> None:
    print("Recuperando metadata desde packs remotos y reconstruyendo DB local...")
    docker_stopan(
        tmp,
        image_tag=image_tag,
        network=network,
        args=[
            "metadata",
            "pack",
            "recover",
            "--config",
            client_config,
            "--object-store",
            str(CONTAINER_ROOT / "client" / "recovered_object_store"),
            "--passphrase-file",
            str(CONTAINER_ROOT / "client" / "metadata.passphrase"),
            "--identity-file",
            str(CONTAINER_ROOT / "client" / "metadata_identity.json"),
            "--pack-out",
            str(CONTAINER_ROOT / "client" / "recovered.stopanmetapack"),
        ],
        timeout=900.0,
    )


def restore_snapshot_and_compare(
    tmp: Path,
    *,
    image_tag: str,
    network: str,
    scenario: Scenario,
    client_config: str,
    snapshot_id: int,
    source_dir: Path,
) -> None:
    print(f"Restaurando snapshot {snapshot_id} con recuperación remota por {scenario.protection_mode}...")
    args = [
        "restore",
        str(snapshot_id),
        "--config",
        client_config,
        "--out",
        str(CONTAINER_ROOT / "restore"),
        "--remote-recovery",
        scenario.protection_mode,
    ]
    if scenario.protection_mode == "replication":
        args.extend(["--replication-targets", str(scenario.remote_copies)])
    output = docker_stopan(tmp, image_tag=image_tag, network=network, args=args, timeout=1200.0)
    restored_dir = host_path_from_container(tmp, final_restore_dir(output))
    assert_same_tree(source_dir, restored_dir)


def run_scenario(
    scenario: Scenario,
    *,
    image_tag: str,
    network_prefix: str,
    dataset_mb: int,
) -> None:
    network = network_name(network_prefix, scenario)
    cleanup_scenario_docker(scenario, network_prefix)
    ensure_network(network)

    tmp = Path(tempfile.mkdtemp(prefix=f"stopan-e2e-{scenario.name}-"))
    success = False

    try:
        print("=" * 90)
        print(f"E2E aceptación Stopan: {scenario.name}")
        print("=" * 90)

        config_dir = tmp / "configs"
        client_dir = tmp / "client"
        source_dir = tmp / "source"
        restore_base = tmp / "restore"
        client_config_host = config_dir / "client.yaml"
        client_config = container_path(tmp, client_config_host)

        for alias in scenario.aliases:
            write_node_config(config_dir / f"{alias}.yaml", scenario=scenario, alias=alias)
        write_client_config(client_config_host, scenario=scenario)
        write_text_file(client_dir / "metadata.passphrase", PASSPHRASE + "\n", mode=0o600)
        create_source_tree(source_dir, dataset_mb=dataset_mb)

        for alias in scenario.aliases:
            start_node(
                tmp,
                image_tag=image_tag,
                network=network,
                scenario=scenario,
                alias=alias,
                config_path=container_path(tmp, config_dir / f"{alias}.yaml"),
            )
            wait_node_ready(scenario, alias)

        wait_cluster_members(
            tmp,
            image_tag=image_tag,
            network=network,
            scenario=scenario,
            client_config=client_config,
        )

        init_metadata(tmp, image_tag=image_tag, network=network, client_config=client_config)
        snapshot_id = create_backup(
            tmp,
            image_tag=image_tag,
            network=network,
            scenario=scenario,
            client_config=client_config,
        )
        assert_metadata_has_backup(client_dir / "_metadata.db", context="después de backup")

        push_chunks(tmp, image_tag=image_tag, network=network, scenario=scenario, client_config=client_config)
        verify_protection(tmp, image_tag=image_tag, network=network, scenario=scenario, client_config=client_config)
        if scenario.protection_mode == "ec":
            assert_ec_metadata_complete(client_dir / "_metadata.db", context="después de push/verify EC")

        export_and_push_metadata_pack(
            tmp,
            image_tag=image_tag,
            network=network,
            scenario=scenario,
            client_config=client_config,
        )
        verify_metadata_pack_distribution(
            tmp,
            image_tag=image_tag,
            network=network,
            client_config=client_config,
        )

        simulate_client_disaster(client_dir, restore_base)
        recover_metadata(tmp, image_tag=image_tag, network=network, client_config=client_config)
        assert_metadata_has_backup(client_dir / "_metadata.db", context="después de recuperar metadata")
        if scenario.protection_mode == "ec":
            assert_ec_metadata_complete(client_dir / "_metadata.db", context="después de recuperar metadata EC")

        restore_snapshot_and_compare(
            tmp,
            image_tag=image_tag,
            network=network,
            scenario=scenario,
            client_config=client_config,
            snapshot_id=snapshot_id,
            source_dir=source_dir,
        )

        success = True
        print(f"E2E {scenario.name} OK")

    except Exception:
        print(f"E2E {scenario.name} FALLÓ. Workdir conservado: {tmp}")
        for alias in scenario.aliases:
            print(f"\n--- Logs {scenario.name}/{alias} ---")
            print(node_logs(scenario, alias))
        raise

    finally:
        cleanup_scenario_docker(scenario, network_prefix)
        if success:
            shutil.rmtree(tmp, ignore_errors=True)


def build_scenarios(choice: str) -> list[Scenario]:
    replication = Scenario(
        name="replication",
        protection_mode="replication",
        node_count=3,
        remote_copies=2,
        metadata_pack_copies=2,
    )
    ec = Scenario(
        name="ec",
        protection_mode="ec",
        node_count=4,
        remote_copies=1,
        metadata_pack_copies=2,
        ec_k=2,
        ec_m=1,
    )
    if choice == "replication":
        return [replication]
    if choice == "ec":
        return [ec]
    return [replication, ec]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="acceptance.py",
        allow_abbrev=False,
        description="Prueba de aceptación E2E completa de Stopan con Docker y paquete Debian.",
    )
    parser.add_argument(
        "--scenario",
        choices=("both", "replication", "ec"),
        default="both",
        help="Escenario a ejecutar. Default: both.",
    )
    parser.add_argument(
        "--dataset-mb",
        type=int,
        default=DEFAULT_DATASET_MB,
        help=f"Tamaño aproximado del dataset generado por escenario. Default: {DEFAULT_DATASET_MB} MiB.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    scenarios = build_scenarios(args.scenario)
    for scenario in scenarios:
        scenario.validate()

    if args.dataset_mb < 32:
        raise E2EError("--dataset-mb debe ser >= 32")

    image_tag = DEFAULT_IMAGE_TAG
    network_prefix = DEFAULT_NETWORK_PREFIX

    build_runtime_image(image_tag)

    for scenario in scenarios:
        run_scenario(
            scenario,
            image_tag=image_tag,
            network_prefix=network_prefix,
            dataset_mb=args.dataset_mb,
        )

    print("=" * 90)
    print("E2E aceptación Stopan OK")
    for scenario in scenarios:
        print(f" - {scenario.name}: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
