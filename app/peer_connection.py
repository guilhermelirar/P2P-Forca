import json
import socket
import threading

from app.config_loader import Config
from app.messages import build_hello
from app.peer_table import Peer


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

    def recv(self) -> dict | None:
        try:
            line = self.rfile.readline()
            if not line:
                return None
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


def connect_to_peer(peer: Peer, config: Config) -> PeerConnection | None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5.0)

    try:
        sock.connect(peer.addr)
    except OSError:
        sock.close()
        return None

    connection = PeerConnection(sock, "outbound", peer.peer_id)
    hello_msg = build_hello(config.peer_id)

    try:
        connection.send(hello_msg)
        resp = connection.recv()
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
