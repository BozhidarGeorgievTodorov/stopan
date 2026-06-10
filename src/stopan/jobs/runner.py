from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

import yaml

_DEFAULT_JOBS_DIR = "/etc/stopan/jobs"
_DEFAULT_LIB_DIR = "/usr/lib/stopan/jobs"
_SUPPORTED_KINDS = {
    "backup",
    "data-push",
    "data-restore",
    "data-verify",
    "metadata-graph",
    "metadata-pack",
    "gc",
}

_TOP_LEVEL_KEYS = {
    "kind",
    "config",
    "backup",
    "data_push",
    "data_restore",
    "data_verify",
    "metadata",
    "metadata_graph",
    "metadata_pack",
    "gc",
}

_SECTION_ENV: dict[str, dict[str, str]] = {
    "backup": {
        "source": "STOPAN_BACKUP_SOURCE",
        "workers": "STOPAN_BACKUP_WORKERS",
        "fast": "STOPAN_BACKUP_FAST",
        "fast_remote": "STOPAN_BACKUP_FAST_REMOTE",
        "safe": "STOPAN_BACKUP_SAFE",
        "deterministic": "STOPAN_BACKUP_DETERMINISTIC",
        "desired_remote_copies": "STOPAN_DESIRED_REMOTE_COPIES",
        "membership_seed": "STOPAN_MEMBERSHIP_SEED",
    },
    "data_push": {
        "protection_mode": "STOPAN_PROTECTION_MODE",
        "scope": "STOPAN_SCOPE",
        "snapshot_id": "STOPAN_SNAPSHOT_ID",
        "limit": "STOPAN_LIMIT",
        "remote_copies": "STOPAN_REMOTE_COPIES",
        "strict_remote_copies": "STOPAN_STRICT_REMOTE_COPIES",
        "ec_k": "STOPAN_EC_K",
        "ec_m": "STOPAN_EC_M",
        "ec_pack_size_bytes": "STOPAN_EC_PACK_SIZE_BYTES",
        "target_parallelism": "STOPAN_TARGET_PARALLELISM",
        "probe_batch_hashes": "STOPAN_PROBE_BATCH_HASHES",
        "stream_inflight": "STOPAN_STREAM_INFLIGHT",
        "probe_timeout_s": "STOPAN_PROBE_TIMEOUT_S",
        "stream_timeout_s": "STOPAN_STREAM_TIMEOUT_S",
        "commit_every": "STOPAN_COMMIT_EVERY",
        "max_message_bytes": "STOPAN_MAX_MESSAGE_BYTES",
        "membership_seed": "STOPAN_MEMBERSHIP_SEED",
    },
    "data_restore": {
        "snapshot_id": "STOPAN_SNAPSHOT_ID",
        "out": "STOPAN_RESTORE_OUT",
        "remote_recovery": "STOPAN_REMOTE_RECOVERY",
        "replication_targets": "STOPAN_REPLICATION_TARGETS",
        "batch_target_parallelism": "STOPAN_BATCH_TARGET_PARALLELISM",
        "prefetch_window": "STOPAN_PREFETCH_WINDOW",
        "membership_seed": "STOPAN_MEMBERSHIP_SEED",
    },
    "data_verify": {
        "protection_mode": "STOPAN_PROTECTION_MODE",
        "scope": "STOPAN_SCOPE",
        "snapshot_id": "STOPAN_SNAPSHOT_ID",
        "limit": "STOPAN_LIMIT",
        "reverify_verified": "STOPAN_REVERIFY_VERIFIED",
        "pack_hash": "STOPAN_PACK_HASH",
        "target_parallelism": "STOPAN_TARGET_PARALLELISM",
        "probe_batch_hashes": "STOPAN_PROBE_BATCH_HASHES",
        "probe_timeout_s": "STOPAN_PROBE_TIMEOUT_S",
        "max_message_bytes": "STOPAN_MAX_MESSAGE_BYTES",
        "membership_seed": "STOPAN_MEMBERSHIP_SEED",
    },
    "metadata": {
        "object_graph": "STOPAN_METADATA_OBJECT_GRAPH",
        "object_store": "STOPAN_METADATA_OBJECT_STORE",
        "passphrase_file": "STOPAN_METADATA_PASSPHRASE_FILE",
        "identity_file": "STOPAN_METADATA_IDENTITY_FILE",
        "object_pack": "STOPAN_METADATA_OBJECT_PACK",
        "object_pack_dir": "STOPAN_METADATA_OBJECT_PACK_DIR",
    },
    "metadata_graph": {
        "action": "STOPAN_METADATA_GRAPH_ACTION",
        "object_store": "STOPAN_METADATA_OBJECT_STORE",
        "passphrase_file": "STOPAN_METADATA_PASSPHRASE_FILE",
        "identity_file": "STOPAN_METADATA_IDENTITY_FILE",
        "decrypt_latest": "STOPAN_METADATA_DECRYPT_LATEST",
        "no_protection": "STOPAN_METADATA_NO_PROTECTION",
        "pack": "STOPAN_METADATA_PACK",
        "pack_out": "STOPAN_METADATA_PACK_OUT",
        "pack_dir": "STOPAN_METADATA_PACK_DIR",
        "default_desired_remote_copies": "STOPAN_DEFAULT_DESIRED_REMOTE_COPIES",
        "scrypt_n": "STOPAN_SCRYPT_N",
        "scrypt_r": "STOPAN_SCRYPT_R",
        "scrypt_p": "STOPAN_SCRYPT_P",
        "key_length": "STOPAN_METADATA_KEY_LENGTH",
    },
    "metadata_pack": {
        "action": "STOPAN_METADATA_PACK_ACTION",
        "path": "STOPAN_METADATA_PACK_PATH",
        "object_store": "STOPAN_METADATA_OBJECT_STORE",
        "pack_out": "STOPAN_METADATA_PACK_OUT",
        "pack_dir": "STOPAN_METADATA_PACK_DIR",
        "pack_in": "STOPAN_METADATA_PACK_IN",
        "passphrase_file": "STOPAN_METADATA_PASSPHRASE_FILE",
        "identity_file": "STOPAN_METADATA_IDENTITY_FILE",
        "owner_id": "STOPAN_OWNER_ID",
        "decrypt": "STOPAN_DECRYPT",
        "expected_pack_hash": "STOPAN_EXPECTED_PACK_HASH",
        "pack_store": "STOPAN_PACK_STORE",
        "pack_hash": "STOPAN_PACK_HASH",
        "local_retrieve_out": "STOPAN_LOCAL_RETRIEVE_OUT",
        "membership_seed": "STOPAN_MEMBERSHIP_SEED",
        "pack_copies": "STOPAN_PACK_COPIES",
        "strict_pack_copies": "STOPAN_STRICT_PACK_COPIES",
        "target_parallelism": "STOPAN_TARGET_PARALLELISM",
        "rpc_timeout_s": "STOPAN_RPC_TIMEOUT_S",
        "max_message_bytes": "STOPAN_MAX_MESSAGE_BYTES",
        "max_candidates": "STOPAN_MAX_CANDIDATES",
        "show_sources": "STOPAN_SHOW_SOURCES",
        "verify_all": "STOPAN_VERIFY_ALL",
        "download_dir": "STOPAN_DOWNLOAD_DIR",
        "download_only": "STOPAN_DOWNLOAD_ONLY",
        "download_pack_out": "STOPAN_DOWNLOAD_PACK_OUT",
        "target_hash": "STOPAN_TARGET_HASH",
        "no_import_db": "STOPAN_NO_IMPORT_DB",
        "no_protection": "STOPAN_NO_PROTECTION",
        "vault_id": "STOPAN_VAULT_ID",
        "scrypt_n": "STOPAN_SCRYPT_N",
        "scrypt_r": "STOPAN_SCRYPT_R",
        "scrypt_p": "STOPAN_SCRYPT_P",
        "key_length": "STOPAN_METADATA_KEY_LENGTH",
        "default_desired_remote_copies": "STOPAN_DEFAULT_DESIRED_REMOTE_COPIES",
    },
    "gc": {
        "target": "STOPAN_GC_TARGET",
        "apply": "STOPAN_GC_APPLY",
        "chunk_store": "STOPAN_CHUNK_STORE",
        "ec_store": "STOPAN_EC_STORE",
        "grace_hours": "STOPAN_GRACE_HOURS",
        "max_age_days": "STOPAN_MAX_AGE_DAYS",
        "object_store": "STOPAN_METADATA_OBJECT_STORE",
        "passphrase_file": "STOPAN_METADATA_PASSPHRASE_FILE",
        "identity_file": "STOPAN_METADATA_IDENTITY_FILE",
        "object_grace_hours": "STOPAN_OBJECT_GRACE_HOURS",
        "pack_grace_hours": "STOPAN_PACK_GRACE_HOURS",
        "pack_dir": "STOPAN_METADATA_PACK_DIR",
        "pack_store": "STOPAN_PACK_STORE",
    },
}

_KIND_SECTION = {
    "backup": "backup",
    "data-push": "data_push",
    "data-restore": "data_restore",
    "data-verify": "data_verify",
    "metadata-graph": "metadata_graph",
    "metadata-pack": "metadata_pack",
    "gc": "gc",
}


def _die(message: str, code: int = 2) -> None:
    print(f"stopan-job: {message}", file=sys.stderr)
    raise SystemExit(code)


def _profile_name(raw: str) -> str:
    if not raw:
        _die("uso: run-job <perfil>")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.@-")
    if any(ch not in allowed for ch in raw):
        _die(f"nombre de perfil no válido: {raw}")
    return raw


def _load_profile(path: Path) -> dict[str, Any]:
    if not path.is_file():
        _die(f"no existe el perfil {path}")
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        _die(f"YAML inválido en {path}: {exc}")
    except OSError as exc:
        _die(f"no se pudo leer {path}: {exc}")
    if data is None:
        _die(f"perfil vacío: {path}")
    if not isinstance(data, dict):
        _die(f"el perfil debe ser un objeto YAML: {path}")
    return data


def _stringify(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (str, int, float)):
        text = str(value)
        return text if text != "" else None
    _die(f"valor de perfil no soportado: {value!r}")


def _apply_section(env: dict[str, str], profile: dict[str, Any], section: str) -> None:
    if section not in profile:
        return
    value = profile[section]
    if value is None:
        return
    if not isinstance(value, dict):
        _die(f"la sección {section} debe ser un objeto YAML")
    mapping = _SECTION_ENV[section]
    unknown = sorted(set(value) - set(mapping))
    if unknown:
        _die(f"claves no soportadas en {section}: {', '.join(unknown)}")
    for key, env_name in mapping.items():
        if key not in value:
            continue
        env_value = _stringify(value[key])
        if env_value is not None:
            env[env_name] = env_value


def _profile_to_env(profile: dict[str, Any], profile_path: Path) -> tuple[str, dict[str, str]]:
    unknown = sorted(set(profile) - _TOP_LEVEL_KEYS)
    if unknown:
        _die(f"claves de nivel superior no soportadas en {profile_path}: {', '.join(unknown)}")

    kind = profile.get("kind")
    if not isinstance(kind, str) or not kind:
        _die(f"kind es obligatorio en {profile_path}")
    if kind not in _SUPPORTED_KINDS:
        _die(f"kind no soportado en {profile_path}: {kind}")

    kind_section = _KIND_SECTION[kind]
    allowed_sections = {kind_section}
    if kind in {"backup", "data-push", "data-verify"}:
        allowed_sections.add("metadata")
    present_sections = set(profile).intersection(_SECTION_ENV)
    invalid_sections = sorted(present_sections - allowed_sections)
    if invalid_sections:
        _die(
            f"secciones no aplicables a kind={kind} en {profile_path}: "
            + ", ".join(invalid_sections)
        )

    env: dict[str, str] = {"STOPAN_JOB_KIND": kind}
    config = _stringify(profile.get("config"))
    if config is not None:
        env["STOPAN_CONFIG"] = config

    # Argumentos metadata comunes para backup/push/verify.
    _apply_section(env, profile, "metadata")
    _apply_section(env, profile, kind_section)
    return kind, env


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    name = _profile_name(args[0] if args else "")
    if len(args) != 1:
        _die("uso: run-job <perfil>")

    jobs_dir = Path(os.environ.get("STOPAN_JOBS_DIR", _DEFAULT_JOBS_DIR))
    lib_dir = Path(os.environ.get("STOPAN_JOB_LIB_DIR", _DEFAULT_LIB_DIR))
    profile_path = jobs_dir / f"{name}.yaml"
    profile = _load_profile(profile_path)
    kind, profile_env = _profile_to_env(profile, profile_path)

    script_path = lib_dir / f"{kind}.sh"
    if not script_path.is_file():
        _die(f"no existe el script de job {script_path}")

    env = os.environ.copy()
    env.update(profile_env)
    env["STOPAN_JOB_NAME"] = name
    env.setdefault("STOPAN_JOB_LIB_DIR", str(lib_dir))

    os.execve(str(script_path), [str(script_path)], env)
    return 127


if __name__ == "__main__":
    raise SystemExit(main())
