"""
Modelos y constantes de identidad de metadata.

La identidad combina una clave Ed25519 para firmas y una clave X25519 para
cifrado/intercambio. Las claves privadas se guardan cifradas en el archivo de
identidad.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from stopan.errors import StopanConfigError


METADATA_IDENTITY_FORMAT = "stopan.metadata_identity"
METADATA_IDENTITY_VERSION = 1
IDENTITY_ALGORITHM_ED25519_X25519 = "ed25519+x25519"
IDENTITY_ALGORITHM_ED25519 = "ed25519"
KDF_SCRYPT = "scrypt"
AEAD_CHACHA20_POLY1305 = "chacha20poly1305"
PRIVATE_KEY_ROLE_SIGNING = "signing"
PRIVATE_KEY_ROLE_ENCRYPTION = "encryption"


class MetadataIdentityError(StopanConfigError, RuntimeError):
    pass


class MetadataIdentityAuthenticationError(MetadataIdentityError):
    pass


@dataclass(frozen=True, slots=True)
class MetadataIdentity:
    owner_id: str
    created_at: str
    signing_public_key_b64: str
    encryption_public_key_b64: str
    algorithm: str = IDENTITY_ALGORITHM_ED25519_X25519
    path: Path | None = None
    private_keys_encrypted: bool = True


@dataclass(frozen=True, slots=True)
class MetadataPrivateIdentity:
    identity: MetadataIdentity
    signing_private_key_raw: bytes
    encryption_private_key_raw: bytes

    def sign(self, data: bytes) -> bytes:
        from stopan.metadata.identity.keys import require_cryptography

        crypto = require_cryptography()
        private_key = crypto.Ed25519PrivateKey.from_private_bytes(self.signing_private_key_raw)
        return private_key.sign(data)
