import json
import socket
import threading


class PeerConnection:
    def __init__(
        self, sock: socket.socket, direction: str, peer_id: str | None = None
    ) -> None:
        self.sock = sock
        self.direction = direction
        self.peer_id = peer_id
        self.closed = False
        self.send_lock = threading.Lock()

    def send(self, msg_dict: dict) -> None:
        msg_json = json.dumps(msg_dict) + "\n"
        with self.send_lock:
            self.sock.sendall(msg_json.encode("utf-8"))

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass
