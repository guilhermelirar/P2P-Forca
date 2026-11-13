import logging
import threading
import time

from app.peer_connection import PeerConnection

logger = logging.getLogger(__name__)


class Peer:
    def __init__(
        self,
        peer_id: str,
        addr: tuple[str, int] | None = None,
        ttl: int | None = None,
        expires_in: int | None = None,
    ) -> None:
        self.peer_id = peer_id
        self.addr = addr
        self.ttl = ttl
        self.expire_at = (
            time.perf_counter() + expires_in if expires_in is not None else float("inf")
        )
        self.state = "DISCOVERED"
        self.conn: PeerConnection | None = None

    def __repr__(self) -> str:
        addr_str = f"{self.addr[0]}:{self.addr[1]}" if self.addr else "unknown"
        return f"{self.peer_id} ({addr_str}) | {self.state}"

    @property
    def is_expired(self) -> bool:
        return time.perf_counter() >= self.expire_at

    @property
    def can_connect(self) -> bool:
        return self.state == "DISCOVERED" and self.addr is not None

    @staticmethod
    def from_dict(d: dict) -> "Peer":
        peer_id = f"{d.get('name')}@{d.get('namespace')}"
        ip = d.get("ip")
        port = d.get("port")
        addr = (ip, port) if isinstance(ip, str) and isinstance(port, int) else None
        ttl = d.get("ttl")
        expires_in = d.get("expires_in")
        return Peer(
            peer_id,
            addr,
            ttl if isinstance(ttl, int) else None,
            expires_in if isinstance(expires_in, int) else None,
        )


class PeerTable:
    def __init__(self, my_id: str) -> None:
        self._table: dict[str, Peer] = {}
        self.lock = threading.Lock()
        self.my_id = my_id

    def upsert_from_discover(self, peers: list[Peer]):
        with self.lock:
            for peer in peers:
                existing = self._table.get(peer.peer_id)

                if not existing:
                    self._table[peer.peer_id] = peer
                    logger.debug(
                        f"[PEER TABLE] peer {peer.peer_id} DISCOVERED and added to table"
                    )
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

    def attach_connection(self, remote_peer_id: str, conn: PeerConnection) -> bool:
        with self.lock:
            peer = self._table.get(remote_peer_id)
            if peer is None:
                peer = Peer(peer_id=remote_peer_id)
                self._table[remote_peer_id] = peer
                logger.debug(
                    f"[PEER TABLE] Peer '{remote_peer_id}' created on {conn.direction} connection"
                )

            # aplicar desempate se existir conexão ativa
            if peer.conn is not None and not peer.conn.closed:
                # menor peer_id mantém outbound; maior peer_id mantém inbound
                keep_outbound = self.my_id < remote_peer_id
                preferred_dir = "outbound" if keep_outbound else "inbound"

                if conn.direction == preferred_dir:
                    logger.info(
                        f"[PEER TABLE] Duplicate connection with {remote_peer_id}"
                        f" — keeping new {conn.direction}, closing old {peer.conn.direction}"
                    )
                    peer.conn.close()
                    peer.conn = conn
                    peer.state = "CONNECTED"
                    return True
                else:
                    logger.info(
                        f"[PEER TABLE] Duplicate connection with {remote_peer_id}"
                        f"— keeping existing {peer.conn.direction}, closing new {conn.direction}"
                    )
                    conn.close()
                    return False

            peer.conn = conn
            peer.state = "CONNECTED"
            logger.debug(
                f"[PEER TABLE] {conn.direction.capitalize()}"
                    f"connection from '{remote_peer_id}' added to peer table"
            )
            return True

    def attach_inbound(self, remote_peer_id: str, conn: PeerConnection) -> bool:
        return self.attach_connection(remote_peer_id, conn)

    def attach_outbound(self, remote_peer_id: str, conn: PeerConnection) -> bool:
        return self.attach_connection(remote_peer_id, conn)

    def all_peers(self) -> list[Peer]:
        with self.lock:
            return list(self._table.values())
