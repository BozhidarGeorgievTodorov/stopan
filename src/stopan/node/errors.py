from __future__ import annotations

from stopan.errors import StopanConfigError, StopanConfigValueError


class NodeIdentityError(StopanConfigError, RuntimeError):
    """Identidad persistente de nodo ausente, corrupta o incoherente."""


class NodeIdentityValueError(StopanConfigValueError):
    """Valor inválido al escribir o actualizar identidad de nodo."""


class MembershipConfigError(StopanConfigValueError):
    """Configuración o dirección de membership inválida."""
