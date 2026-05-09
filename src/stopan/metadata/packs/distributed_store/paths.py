from __future__ import annotations

from pathlib import Path

from stopan.metadata.identity import validate_owner_id
from stopan.metadata.packs.format import OBJECT_PACK_FILE_SUFFIX
from stopan.metadata.packs.hashes import validate_pack_hash


def pack_path_for(*, root_dir: str | Path, owner_id: str, pack_hash: str) -> Path:
    root = Path(root_dir).expanduser().resolve()
    owner = validate_owner_id(owner_id)
    pack = validate_pack_hash(pack_hash)
    return root / owner[:2] / owner / pack[:2] / f"{pack}{OBJECT_PACK_FILE_SUFFIX}"


def signature_path_for(*, root_dir: str | Path, owner_id: str, pack_hash: str) -> Path:
    pack_path = pack_path_for(root_dir=root_dir, owner_id=owner_id, pack_hash=pack_hash)
    return pack_path.with_name(f"{pack_path.name}.signature.json")
