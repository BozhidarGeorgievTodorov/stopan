"""Coordinación local del ciclo de vida y drenaje del nodo.

El proceso de nodo expone un socket Unix local para dos fines:

- registrar operaciones CLI ya iniciadas para que una parada ordenada espere a
  que terminen;
- solicitar la parada local sin exponer una RPC remota de administración.

El mismo controlador protege las RPC de datos: cuando comienza el drenaje se
rechazan llamadas nuevas, mientras las ya admitidas conservan su ejecución.
"""

from __future__ import annotations

import os
import socket
import stat
import tempfile
import threading
from collections.abc import Iterator
from contextvars import ContextVar, Token
from pathlib import Path

import grpc

from stopan.errors import StopanNetworkError


_CONTROL_SOCKET_ENV = "STOPAN_NODE_CONTROL_SOCKET"
_CONTROL_SOCKET_NAME = "control.sock"
_CONTROL_LINE_LIMIT = 4096
_CONTROL_INITIAL_TIMEOUT_S = 3.0
_CONTROL_ACCEPT_TIMEOUT_S = 0.2
_CONTROL_WORKER_JOIN_TIMEOUT_S = _CONTROL_INITIAL_TIMEOUT_S + 1.0
_CONTROL_BACKLOG = 64
_NODE_ID_HEX = frozenset("0123456789abcdef")

_ACTIVE_OPERATION_IDENTITY: ContextVar[tuple[str, str] | None] = ContextVar(
    "stopan_active_operation_identity",
    default=None,
)


class NodeDrainController:
    """Estado compartido entre el control local y la admisión de RPC."""

    def __init__(self) -> None:
        self._draining = threading.Event()
        self._stopped = threading.Event()
        self._condition = threading.Condition()
        self._active_local_operations = 0
        self._active_rpcs = 0

    @property
    def draining(self) -> bool:
        return self._draining.is_set()

    @property
    def stopped(self) -> bool:
        return self._stopped.is_set()

    def request_draining(self) -> bool:
        """Cierra atómicamente la admisión de trabajo nuevo.

        El mismo cerrojo protege la transición a drenaje y los contadores de
        trabajo. Así, cuando este método retorna, ninguna operación local ni RPC
        puede haber cruzado la barrera de admisión después de la solicitud.
        """
        with self._condition:
            first_request = not self._draining.is_set()
            self._draining.set()
            self._condition.notify_all()
            return first_request

    def try_begin_local_operation(self) -> bool:
        """Registra una operación local si el drenaje todavía no ha comenzado."""
        with self._condition:
            if self._draining.is_set():
                return False
            self._active_local_operations += 1
            return True

    def end_local_operation(self) -> None:
        with self._condition:
            if self._active_local_operations <= 0:
                raise RuntimeError("contador de operaciones locales incoherente")
            self._active_local_operations -= 1
            self._condition.notify_all()

    def admit_rpc(self, context) -> None:
        """Registra una RPC autenticada o la rechaza si el nodo está drenando.

        El contador se libera mediante ``ServicerContext.add_callback`` para
        cubrir por igual RPC unarias y streaming, incluidas cancelaciones.
        """
        with self._condition:
            rejected = self._draining.is_set()
            if not rejected:
                self._active_rpcs += 1

        if rejected:
            context.abort(
                grpc.StatusCode.UNAVAILABLE,
                "el nodo está en drenaje y no admite trabajo nuevo",
            )

        released = False
        release_lock = threading.Lock()

        def release() -> None:
            nonlocal released
            with release_lock:
                if released:
                    return
                released = True
            with self._condition:
                if self._active_rpcs <= 0:
                    raise RuntimeError("contador de RPC activas incoherente")
                self._active_rpcs -= 1
                self._condition.notify_all()

        try:
            registered = context.add_callback(release)
        except Exception:
            release()
            raise

        if registered is False:
            release()
            context.abort(grpc.StatusCode.CANCELLED, "la RPC ya no está activa")

    def active_work(self) -> tuple[int, int]:
        """Devuelve (operaciones locales, RPC activas) para diagnóstico."""
        with self._condition:
            return self._active_local_operations, self._active_rpcs

    def wait_for_idle(self) -> None:
        """Espera hasta que todo el trabajo admitido antes del drenaje termine."""
        with self._condition:
            while self._active_local_operations or self._active_rpcs:
                self._condition.wait()

    def mark_stopped(self) -> None:
        self._stopped.set()
        with self._condition:
            self._condition.notify_all()

    def wait_stopped(self, timeout: float | None = None) -> bool:
        return self._stopped.wait(timeout=timeout)


def _fallback_runtime_dir() -> Path:
    xdg_runtime = str(os.environ.get("XDG_RUNTIME_DIR", "")).strip()
    if xdg_runtime:
        return Path(xdg_runtime) / "stopan"
    return Path(tempfile.gettempdir()) / f"stopan-{os.getuid()}"


def _control_socket_candidates() -> tuple[Path, ...]:
    explicit = str(os.environ.get(_CONTROL_SOCKET_ENV, "")).strip()
    if explicit:
        return (Path(explicit),)

    system_path = Path("/run/stopan") / _CONTROL_SOCKET_NAME
    fallback = _fallback_runtime_dir() / _CONTROL_SOCKET_NAME

    # Debe reflejar la selección del servidor. Un proceso sin permisos sobre
    # /run/stopan usa su runtime de usuario, por lo que ese candidato se prueba
    # primero y no queda bloqueado por un directorio systemd inaccesible.
    system_parent = system_path.parent
    if system_parent.is_dir() and os.access(system_parent, os.W_OK | os.X_OK):
        candidates = [system_path, fallback]
    else:
        candidates = [fallback, system_path]

    return tuple(dict.fromkeys(candidates))


def _validate_fallback_runtime_directory(parent: Path, *, create: bool) -> None:
    """Asegura que el runtime de usuario no pueda ser secuestrado en un directorio compartido."""
    if create:
        try:
            parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        except OSError as exc:
            raise StopanNetworkError(
                f"No se pudo preparar el runtime local del nodo en {parent}: {exc}"
            ) from exc

    try:
        st = parent.lstat()
    except FileNotFoundError:
        return
    except OSError as exc:
        raise StopanNetworkError(
            f"No se pudo inspeccionar el runtime local del nodo en {parent}: {exc}"
        ) from exc

    if not stat.S_ISDIR(st.st_mode):
        raise StopanNetworkError(
            f"El runtime local del nodo no es un directorio real: {parent}"
        )
    if st.st_uid != os.geteuid():
        raise StopanNetworkError(
            f"El runtime local del nodo pertenece a otro usuario: {parent}"
        )

    unsafe_mode = stat.S_IMODE(st.st_mode) & 0o077
    if unsafe_mode:
        if not create:
            raise StopanNetworkError(
                f"El runtime local del nodo tiene permisos inseguros: {parent}"
            )
        try:
            os.chmod(parent, 0o700)
        except OSError as exc:
            raise StopanNetworkError(
                f"No se pudo restringir el runtime local del nodo en {parent}: {exc}"
            ) from exc


def _prepare_runtime_directory(path: Path) -> None:
    parent = path.parent
    fallback = _fallback_runtime_dir()
    if parent == fallback:
        _validate_fallback_runtime_directory(parent, create=True)
        return

    try:
        parent.mkdir(mode=0o750, parents=True, exist_ok=True)
    except OSError as exc:
        raise StopanNetworkError(
            f"No se pudo preparar el directorio de control del nodo en {parent}: {exc}"
        ) from exc


def select_control_socket_path_for_server() -> Path:
    """Elige una ruta escribible para el socket de control del proceso actual."""
    explicit = str(os.environ.get(_CONTROL_SOCKET_ENV, "")).strip()
    if explicit:
        path = Path(explicit)
        _prepare_runtime_directory(path)
        return path

    system_path = Path("/run/stopan") / _CONTROL_SOCKET_NAME
    system_parent = system_path.parent
    if system_parent.is_dir() and os.access(system_parent, os.W_OK | os.X_OK):
        return system_path

    fallback = _fallback_runtime_dir() / _CONTROL_SOCKET_NAME
    _prepare_runtime_directory(fallback)
    return fallback


def _existing_control_socket_paths() -> Iterator[Path]:
    fallback = _fallback_runtime_dir()
    for path in _control_socket_candidates():
        if path.parent == fallback and path.parent.exists():
            _validate_fallback_runtime_directory(path.parent, create=False)
        try:
            mode = path.lstat().st_mode
        except FileNotFoundError:
            continue
        except PermissionError as exc:
            raise StopanNetworkError(
                f"Sin permiso para inspeccionar el control local del nodo en {path}. "
                "Ejecuta la operación con un usuario autorizado para el servicio."
            ) from exc
        except OSError as exc:
            # Si no podemos determinar con certeza si existe un endpoint de
            # control, continuar sin lease rompería la garantía de drenaje.
            raise StopanNetworkError(
                f"No se pudo inspeccionar de forma segura el control local del nodo en {path}: {exc}"
            ) from exc
        if not stat.S_ISSOCK(mode):
            # Una entrada inesperada en la ruta de control es ambigua. Tratarla
            # como "daemon ausente" permitiría iniciar trabajo sin lease aunque
            # el endpoint real hubiera sido sustituido o manipulado.
            raise StopanNetworkError(
                f"La ruta de control local existe y no es un socket Unix: {path}"
            )
        yield path



def _path_identity(path: Path) -> tuple[int, int] | None:
    """Devuelve (device, inode) de la ruta o None si ya no existe."""
    try:
        st = path.lstat()
    except FileNotFoundError:
        return None
    return st.st_dev, st.st_ino


def _unlink_socket_if_unchanged(path: Path, identity: tuple[int, int]) -> bool:
    """Elimina un socket solo si la entrada de directorio sigue siendo la observada."""
    try:
        st = path.lstat()
    except FileNotFoundError:
        return False
    if not stat.S_ISSOCK(st.st_mode) or (st.st_dev, st.st_ino) != identity:
        return False
    path.unlink()
    return True


def _recv_line(sock: socket.socket, *, limit: int = _CONTROL_LINE_LIMIT) -> str:
    data = bytearray()
    while len(data) < limit:
        chunk = sock.recv(1)
        if not chunk:
            break
        if chunk == b"\n":
            break
        data.extend(chunk)
    if len(data) >= limit:
        raise StopanNetworkError("Respuesta inválida del control local del nodo: línea demasiado larga")
    return data.decode("utf-8", errors="strict")


def _connect_control_socket(*, required: bool) -> socket.socket | None:
    if not hasattr(socket, "AF_UNIX"):
        if required:
            raise StopanNetworkError("El control local del nodo requiere sockets Unix en esta versión")
        return None

    last_error: OSError | None = None
    found = False
    ambiguous_failure = False
    for path in _existing_control_socket_paths():
        found = True
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.settimeout(_CONTROL_INITIAL_TIMEOUT_S)
        try:
            client.connect(str(path))
            return client
        except (ConnectionRefusedError, FileNotFoundError) as exc:
            # Un socket huérfano de un daemon ya muerto no debe impedir el modo
            # CLI sin daemon. El servidor lo reclamará en el siguiente arranque.
            last_error = exc
            client.close()
            continue
        except PermissionError as exc:
            client.close()
            raise StopanNetworkError(
                f"Sin permiso para usar el control local del nodo en {path}. "
                "El usuario debe poder acceder al runtime del servicio."
            ) from exc
        except OSError as exc:
            # Si existe un endpoint pero falla de una forma distinta a un socket
            # huérfano, continuar sin lease perdería la garantía de drenaje.
            last_error = exc
            ambiguous_failure = True
            client.close()
            continue

    if ambiguous_failure:
        raise StopanNetworkError(
            "No se pudo usar de forma segura el control local del nodo"
            + (f": {last_error}" if last_error else "")
        )
    if required:
        detail = f": {last_error}" if found and last_error else ""
        raise StopanNetworkError(f"No hay un nodo local en ejecución con control disponible{detail}")
    return None


def _normalize_control_node_id(value: str | None) -> str | None:
    node_id = str(value or "").strip()
    if not node_id:
        return None
    if len(node_id) != 32 or any(char not in _NODE_ID_HEX for char in node_id):
        raise StopanNetworkError("Identidad inválida recibida del control local del nodo")
    return node_id


def _normalize_control_address(value: str | None) -> str | None:
    address = str(value or "").strip()
    if not address:
        return None
    if len(address) > 1024 or any(char in address for char in "\r\n\t"):
        raise StopanNetworkError("Dirección inválida recibida del control local del nodo")
    return address


def current_local_operation_identity() -> tuple[str, str] | None:
    """Devuelve (node_id, advertise_addr) del daemon que concedió la lease activa."""
    return _ACTIVE_OPERATION_IDENTITY.get()


def current_local_operation_node_id() -> str | None:
    """Devuelve el node_id del daemon que concedió la lease de esta operación."""
    identity = current_local_operation_identity()
    return identity[0] if identity is not None else None


def current_local_operation_matches(self_addr: str | None) -> bool:
    """Indica si la operación actual fue admitida por el daemon de self_addr."""
    identity = current_local_operation_identity()
    if identity is None:
        return False
    address = str(self_addr or "").strip()
    return bool(address and address == identity[1])


class LocalOperationLease:
    """Conexión mantenida durante una operación CLI para participar en el drenaje."""

    def __init__(
        self,
        sock: socket.socket | None,
        *,
        node_id: str | None = None,
        advertise_addr: str | None = None,
    ):
        self._sock = sock
        self.node_id = _normalize_control_node_id(node_id)
        self.advertise_addr = _normalize_control_address(advertise_addr)
        if (self.node_id is None) != (self.advertise_addr is None):
            raise StopanNetworkError("Identidad incompleta recibida del control local del nodo")
        self._context_token: Token[tuple[str, str] | None] | None = None

    def close(self) -> None:
        sock, self._sock = self._sock, None
        if sock is None:
            return
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        sock.close()

    def __enter__(self) -> "LocalOperationLease":
        if self._context_token is not None:
            raise RuntimeError("la lease de operación local ya está activa en este contexto")
        if self.node_id is not None and self.advertise_addr is not None:
            self._context_token = _ACTIVE_OPERATION_IDENTITY.set(
                (self.node_id, self.advertise_addr)
            )
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        token, self._context_token = self._context_token, None
        if token is not None:
            _ACTIVE_OPERATION_IDENTITY.reset(token)
        self.close()


def acquire_local_operation_lease(operation: str) -> LocalOperationLease:
    """Registra una operación si existe un nodo local, o actúa como no-op si no existe."""
    sock = _connect_control_socket(required=False)
    if sock is None:
        return LocalOperationLease(None)

    operation_name = "".join(ch for ch in str(operation) if ch.isalnum() or ch in "-_.")[:128] or "operation"
    try:
        sock.sendall(f"BEGIN\t{os.getpid()}\t{operation_name}\n".encode("utf-8"))
        response = _recv_line(sock)
        if response == "OK":
            sock.settimeout(None)
            return LocalOperationLease(sock)
        if response.startswith("OK\t"):
            fields = response.split("\t")
            if len(fields) != 3:
                raise StopanNetworkError(
                    f"Respuesta inválida del control local del nodo: {response!r}"
                )
            node_id = _normalize_control_node_id(fields[1])
            advertise_addr = _normalize_control_address(fields[2])
            sock.settimeout(None)
            return LocalOperationLease(
                sock,
                node_id=node_id,
                advertise_addr=advertise_addr,
            )
        if response == "DRAINING":
            raise StopanNetworkError(
                "El nodo local está en drenaje y no admite operaciones nuevas. "
                "Espera a que termine la parada y vuelve a intentarlo."
            )
        raise StopanNetworkError(f"Respuesta inesperada del control local del nodo: {response!r}")
    except StopanNetworkError:
        sock.close()
        raise
    except (OSError, UnicodeError) as exc:
        sock.close()
        raise StopanNetworkError(
            f"Falló el control local al registrar la operación {operation_name!r}: {exc}"
        ) from exc
    except Exception:
        sock.close()
        raise


def request_local_node_stop() -> None:
    """Solicita una parada ordenada local y espera hasta que el proceso termine."""
    sock = _connect_control_socket(required=True)
    assert sock is not None
    try:
        try:
            sock.sendall(f"STOP\t{os.getpid()}\n".encode("utf-8"))
            response = _recv_line(sock)
        except (OSError, UnicodeError) as exc:
            raise StopanNetworkError(
                f"Falló el control local al solicitar la parada: {exc}"
            ) from exc

        if response != "STOPPING":
            raise StopanNetworkError(f"Respuesta inesperada al solicitar parada: {response!r}")

        sock.settimeout(None)
        try:
            response = _recv_line(sock)
        except (OSError, UnicodeError) as exc:
            raise StopanNetworkError(
                f"Se perdió el control local mientras se esperaba la parada: {exc}"
            ) from exc
        if response != "STOPPED":
            raise StopanNetworkError(
                "El nodo aceptó la parada, pero el canal de control terminó antes de confirmar el cierre"
            )
    finally:
        sock.close()


class NodeControlServer:
    """Servidor Unix local que coordina leases CLI y solicitudes de parada."""

    def __init__(
        self,
        *,
        controller: NodeDrainController,
        request_shutdown,
        socket_path: Path | None = None,
        node_id: str | None = None,
        advertise_addr: str | None = None,
    ) -> None:
        self._controller = controller
        self._request_shutdown = request_shutdown
        self.socket_path = socket_path or select_control_socket_path_for_server()
        self._node_id = _normalize_control_node_id(node_id) or ""
        self._advertise_addr = _normalize_control_address(advertise_addr) or ""
        if bool(self._node_id) != bool(self._advertise_addr):
            raise StopanNetworkError(
                "NodeControlServer requiere node_id y advertise_addr conjuntamente"
            )
        self._listener: socket.socket | None = None
        self._socket_identity: tuple[int, int] | None = None
        self._accept_thread: threading.Thread | None = None
        self._closing = threading.Event()
        self._workers_lock = threading.Lock()
        self._workers: set[threading.Thread] = set()

    def start(self) -> None:
        if not hasattr(socket, "AF_UNIX"):
            raise StopanNetworkError("El control local del nodo requiere sockets Unix en esta versión")
        if self._listener is not None:
            raise RuntimeError("el servidor de control local ya está iniciado")

        path = self.socket_path
        _prepare_runtime_directory(path)
        try:
            st = path.lstat()
        except FileNotFoundError:
            stale_identity = None
        except OSError as exc:
            raise StopanNetworkError(
                f"No se pudo inspeccionar el socket de control existente {path}: {exc}"
            ) from exc
        else:
            if not stat.S_ISSOCK(st.st_mode):
                raise StopanNetworkError(
                    f"La ruta de control del nodo existe y no es un socket Unix: {path}"
                )
            stale_identity = (st.st_dev, st.st_ino)

        if stale_identity is not None:
            probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            probe.settimeout(0.25)
            try:
                probe.connect(str(path))
            except (ConnectionRefusedError, FileNotFoundError):
                # Solo se elimina la misma entrada que se comprobó como huérfana.
                try:
                    removed = _unlink_socket_if_unchanged(path, stale_identity)
                except OSError as exc:
                    raise StopanNetworkError(
                        f"No se pudo retirar el socket de control huérfano {path}: {exc}"
                    ) from exc
                if not removed and path.exists():
                    raise StopanNetworkError(
                        f"El socket de control {path} cambió mientras se verificaba; "
                        "se preserva la instancia concurrente"
                    )
            except OSError as exc:
                # Ante un resultado ambiguo se preserva la instancia existente.
                raise StopanNetworkError(
                    f"No se puede verificar el socket de control existente {path}: {exc}"
                ) from exc
            else:
                raise StopanNetworkError(
                    f"Ya existe un nodo local activo con socket de control en {path}"
                )
            finally:
                probe.close()

        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        bound_identity: tuple[int, int] | None = None
        try:
            listener.bind(str(path))
            bound_identity = _path_identity(path)
            if bound_identity is None:
                raise StopanNetworkError(
                    f"El socket de control desapareció inmediatamente después de crearse: {path}"
                )
            os.chmod(path, 0o660)
            listener.listen(_CONTROL_BACKLOG)
            listener.settimeout(_CONTROL_ACCEPT_TIMEOUT_S)
            self._socket_identity = bound_identity
        except Exception as exc:
            listener.close()
            if bound_identity is not None:
                try:
                    _unlink_socket_if_unchanged(path, bound_identity)
                except OSError:
                    pass
            if isinstance(exc, OSError):
                raise StopanNetworkError(
                    f"No se pudo abrir el socket de control local {path}: {exc}"
                ) from exc
            raise

        self._listener = listener
        self._closing.clear()
        accept_thread = threading.Thread(
            target=self._accept_loop,
            name="node-control",
            daemon=True,
        )
        try:
            accept_thread.start()
        except Exception:
            self._listener = None
            listener.close()
            if self._socket_identity is not None:
                try:
                    _unlink_socket_if_unchanged(path, self._socket_identity)
                except OSError:
                    pass
            self._socket_identity = None
            raise
        self._accept_thread = accept_thread

    def _accept_loop(self) -> None:
        while not self._closing.is_set():
            listener = self._listener
            if listener is None:
                return
            try:
                conn, _ = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                if self._closing.is_set():
                    return
                # Un fallo transitorio de accept no debe convertir este hilo en
                # un bucle ocupado. Se reintenta con la misma cadencia del
                # timeout normal mientras el servidor siga abierto.
                if self._closing.wait(_CONTROL_ACCEPT_TIMEOUT_S):
                    return
                continue

            worker = threading.Thread(
                target=self._handle_connection,
                args=(conn,),
                name="node-control-client",
                daemon=True,
            )
            with self._workers_lock:
                self._workers.add(worker)
            try:
                worker.start()
            except Exception:
                with self._workers_lock:
                    self._workers.discard(worker)
                try:
                    conn.close()
                except OSError:
                    pass

    def _handle_connection(self, conn: socket.socket) -> None:
        current = threading.current_thread()
        local_operation_registered = False
        try:
            conn.settimeout(_CONTROL_INITIAL_TIMEOUT_S)
            line = _recv_line(conn)
            parts = line.split("\t")
            command = parts[0] if parts else ""

            if command == "BEGIN" and len(parts) == 3 and parts[1].isdigit() and parts[2]:
                if not self._controller.try_begin_local_operation():
                    conn.sendall(b"DRAINING\n")
                    return

                local_operation_registered = True
                response = (
                    f"OK\t{self._node_id}\t{self._advertise_addr}\n"
                    if self._node_id
                    else "OK\n"
                )
                conn.sendall(response.encode("utf-8"))
                conn.settimeout(None)
                while conn.recv(1024):
                    pass
                return

            if command == "STOP" and len(parts) == 2 and parts[1].isdigit():
                self._controller.request_draining()
                self._request_shutdown()
                conn.sendall(b"STOPPING\n")
                conn.settimeout(None)
                self._controller.wait_stopped()
                try:
                    conn.sendall(b"STOPPED\n")
                except OSError:
                    pass
                return

            conn.sendall(b"ERROR\n")
        except (OSError, UnicodeError, StopanNetworkError):
            pass
        finally:
            if local_operation_registered:
                self._controller.end_local_operation()
            try:
                conn.close()
            except OSError:
                pass
            with self._workers_lock:
                self._workers.discard(current)

    def close(self) -> None:
        self._closing.set()
        listener, self._listener = self._listener, None
        if listener is not None:
            try:
                listener.close()
            except OSError:
                pass

        thread = self._accept_thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=1.0)
        self._accept_thread = None

        with self._workers_lock:
            workers = list(self._workers)
        for worker in workers:
            if worker is threading.current_thread():
                continue
            # Tras mark_stopped(), los STOP despiertan de inmediato y las
            # conexiones que aún no han enviado comando tienen como máximo el
            # timeout inicial. Dar ese margen evita cortar la confirmación
            # STOPPED al cliente por un simple retardo de scheduling.
            worker.join(timeout=_CONTROL_WORKER_JOIN_TIMEOUT_S)

        try:
            st = self.socket_path.lstat()
        except FileNotFoundError:
            self._socket_identity = None
            return
        except OSError:
            self._socket_identity = None
            return

        identity = (st.st_dev, st.st_ino)
        if stat.S_ISSOCK(st.st_mode) and identity == self._socket_identity:
            try:
                self.socket_path.unlink()
            except OSError:
                pass
        self._socket_identity = None
