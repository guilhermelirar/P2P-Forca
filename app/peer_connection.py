import json
import logging
import socket
import threading

from app.config_loader import Config
from app.messages import build_hello, build_hello_ok

logger = logging.getLogger(__name__)

class PeerConnection:
    def __init__(
        self, sock: socket.socket, direction: str, peer_id: str | None = None
    ) -> None:
        self.sock = sock
        self.rfile = sock.makefile("r", encoding="utf-8")
        self.direction = direction
        self.peer_id = peer_id
        self.closed = False
        self.send_lock = threading.Lock()

    def send(self, msg_dict: dict) -> None:
        msg_json = json.dumps(msg_dict) + "\n"
        with self.send_lock:
            self.sock.sendall(msg_json.encode("utf-8"))

    def recv_json(self) -> dict | None:
        try:
            line = self.rfile.readline()
            if not line:
                return None
            logger.debug(f"[Received]: {line}\n")
            return json.loads(line)
        except (OSError, json.JSONDecodeError):
            return None

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.rfile.close()
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass


def connect_to_peer(
    peer_id: str, peer_addr: tuple[str, int], config: Config
) -> PeerConnection | None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5.0)

    try:
        sock.connect(peer_addr)
    except OSError:
        sock.close()
        return None

    connection = PeerConnection(sock, "outbound", peer_id)
    hello_msg = build_hello(config.peer_id)

    try:
        connection.send(hello_msg)
        resp = connection.recv_json()
    except OSError:
        connection.close()
        return None

    if (
        resp
        and resp.get("type") == "HELLO_OK"
        and resp.get("msg_id") == hello_msg["msg_id"]
        and "hangman" in resp.get("features", [])
    ):
        sock.settimeout(None)
        return connection

    connection.close()
    return None


def start_server(
    config: Config,
    on_incoming_connection,
    stop_event: threading.Event,
) -> socket.socket:
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(("0.0.0.0", config.listen_port))
    server_sock.listen()

    def _accept_loop() -> None:
        while not stop_event.is_set():
            try:
                client_sock, _ = server_sock.accept()
            except OSError:
                break

            client_sock.settimeout(5.0)
            conn = PeerConnection(client_sock, "inbound")
            msg = conn.recv_json()

            if (
                msg
                and msg.get("type") == "HELLO"
                and "hangman" in msg.get("features", [])
                and msg.get("peer_id")
            ):
                remote_peer_id = msg["peer_id"]
                conn.peer_id = remote_peer_id
                try:
                    conn.send(build_hello_ok(msg["msg_id"], config.peer_id))
                    client_sock.settimeout(None)
                    on_incoming_connection(remote_peer_id, conn)
                except OSError:
                    conn.close()
            else:
                conn.close()

    threading.Thread(target=_accept_loop, daemon=True).start()
    return server_sock
