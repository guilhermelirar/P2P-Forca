import json
import logging
import socket

from app.config_loader import Config, RDVServer
from app.messages import RendezvousMessage
from app.peer_table import Peer

logger = logging.getLogger(__name__)

MAX_MESSAGE_LEN_BYTES = 32 * 1024  # 32 KB
RDV_TIMEOUT = 5.0


def exchange_message(
    server: RDVServer, message: RendezvousMessage
) -> RendezvousMessage:
    if not message.type:
        raise ValueError(
            "Mensagem precisa ser de um tipo (REGISTER / UNREGISTER / DISCOVER / REFRESH)"
        )

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(RDV_TIMEOUT)
        sock.connect(server)

        message_encoded = message.to_json().encode()
        sock.sendall(message_encoded)
        logger.debug(f"Sent {message.type}:\n {message.to_json()}")

        response_bytes = sock.recv(MAX_MESSAGE_LEN_BYTES)
        if not response_bytes:
            raise RuntimeError("Empty response from Rendezvous server")

        response_dict = json.loads(response_bytes.decode("utf-8"))
        logger.debug(f"Received :\n {response_dict}")
        return RendezvousMessage.from_dict(response_dict)


def register(config: Config) -> tuple[str, int] | None:
    try:
        response = exchange_message(
            config.rdv_server, RendezvousMessage.build_register(config)
        )

        if response.is_ok():
            if not response.ip or not response.ttl:
                raise RuntimeError(
                    "REGISTER OK but no info returned (expected ip and ttl)"
                )

            logger.info(
                "[RDV]"
                f" registered {config.peer_id} as"
                f" {response.ip}:{response.port}"
                f" (ttl={response.ttl})"
            )

            return response.ip, response.ttl

        logger.error(f"[RDV] Error at REGISTER: {response.message}")
    except (OSError, json.JSONDecodeError, ValueError, RuntimeError) as e:
        logger.warning("[RDV] Communication fail at REGISTER: %s", e)

    return None


def discover(
    server: RDVServer, my_pub_ip: str, my_id: str, namespace: str | None = None
) -> list[Peer] | None:
    try:
        response = exchange_message(server, RendezvousMessage.build_discover(namespace))
        if response.is_ok():
            if response.peers is None:
                raise RuntimeError("DISCOVER OK but no 'peers' field")

            peers: list[Peer] = [
                peer
                for peer in (Peer.from_dict(p) for p in response.peers)
                if peer.peer_id != my_id
            ]
            for p in peers:
                if p.addr and p.addr[0] == my_pub_ip:
                    p.addr = ("127.0.0.1", p.addr[1])

            peers_str = "\n\t".join(str(p) for p in peers)
            logger.debug("[RDV] Discovered %d peer(s):\n\t%s", len(peers), peers_str)
            return peers

        logger.error("[RDV] Error at DISCOVER: %s", response.message)
    except (OSError, json.JSONDecodeError, ValueError, RuntimeError) as exc:
        logger.warning("[RDV] Communication fail at DISCOVER: %s", exc)
    return None


def unregister(server: RDVServer, namespace: str, name: str, port: int) -> bool:
    try:
        response = exchange_message(
            server, RendezvousMessage.build_unregister(namespace, name, port)
        )
        if response.is_ok():
            logger.info("[RDV] Peer unregistered")
            return True

        logger.error("[RDV] Error when UNREGISTER: %s", response.message)
    except (OSError, json.JSONDecodeError, ValueError, RuntimeError) as exc:
        logger.warning("[RDV] Communication fail at unregister: %s", exc)
    return False
