import threading
import time

from app.peer_connection import PeerConnection


class Peer:
    def __init__(
        self, ip: str, port: int, name: str, namespace: str, ttl: int, expires_in: int
    ) -> None:
        self.addr: tuple[str, int] = (ip, port)
        self.name = name
        self.namespace = namespace
        self.ttl = ttl
        self.expire_at = time.perf_counter() + expires_in
        self.state = "DISCOVERED"
        self.conn: None | PeerConnection = None

    def __repr__(self) -> str:
        return f"{self.name}@{self.namespace} ({self.addr[0]}:{self.addr[1]}) | {self.state}"

    @property
    def is_expired(self) -> bool:
        return time.perf_counter() >= self.expire_at

    @property
    def peer_id(self):
        return f"{self.name}@{self.namespace}"

    @staticmethod
    def from_dict(d):
        ip = d.get("ip")
        port = d.get("port")
        name = d.get("name")
        namespace = d.get("namespace")
        ttl = d.get("ttl")
        expires_in = d.get("expires_in")

        return Peer(ip, port, name, namespace, ttl, expires_in)


class PeerTable:
    def __init__(self) -> None:
        self._table: dict[str, Peer] = {}
        self.lock = threading.Lock()

    def upsert_from_discover(self, peers: list[Peer]):
        with self.lock:
            for peer in peers:
                existing = self._table.get(peer.peer_id)

                if not existing:
                    self._table[peer.peer_id] = peer
                    continue

                if existing.addr != peer.addr and existing.conn is not None:
                    # existing.conn.bye()
                    existing.conn.close()
                    existing.conn = None
                    existing.state = "DISCOVERED"

                existing.addr = peer.addr
                existing.ttl = peer.ttl
                existing.expire_at = peer.expire_at

            discovered_ids = {p.peer_id for p in peers}
            gone_peers = [
                peer for pid, peer in self._table.items() if pid not in discovered_ids
            ]

            for gone in gone_peers:
                if gone.conn:
                    gone.conn.close()
                    gone.state = "GONE"
