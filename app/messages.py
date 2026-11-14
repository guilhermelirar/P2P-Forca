import json
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime

from app.config_loader import Config


@dataclass
class RendezvousMessage:
    # REQUEST
    type: str | None = None
    namespace: str | None = None
    name: str | None = None
    port: int | None = None
    ttl: int | None = None

    # RESPONSE
    status: str | None = None
    message: str | None = None
    ip: str | None = None
    peers: list[dict] | None = None

    def to_dict(self) -> dict:
        return {k: v for k, v in asdict(self).items() if v is not None}

    def to_json(self) -> str:
        return json.dumps(self.to_dict()) + "\n"

    def is_ok(self) -> bool:
        return self.status == "OK"  # "ERROR" caso contrário

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**{k: v for k, v in data.items() if k in cls.__annotations__})

    @classmethod
    def build_register(cls, config: Config):
        return cls(
            type="REGISTER",
            namespace=config.namespace,
            name=config.name,
            port=config.listen_port,
            ttl=config.register_ttl,
        )

    @classmethod
    def build_discover(cls, namespace: str | None):
        return cls(type="DISCOVER", namespace=namespace)

    @classmethod
    def build_unregister(cls, namespace: str, name: str, port: int):
        return cls(type="UNREGISTER", namespace=namespace, name=name, port=port)


def build_hello(
    peer_id: str,
    version: str = "1.0",
    features: list[str] | None = None,
    ttl: int = 1,
) -> dict:
    return {
        "type": "HELLO",
        "msg_id": str(uuid.uuid4()),
        "peer_id": peer_id,
        "version": version,
        "features": features if features is not None else ["hangman"],
        "ttl": ttl,
    }


def build_hello_ok(
    msg_id: str,
    peer_id: str,
    version: str = "1.0",
    features: list[str] | None = None,
    ttl: int = 1,
) -> dict:
    return {
        "type": "HELLO_OK",
        "msg_id": msg_id,
        "peer_id": peer_id,
        "version": version,
        "features": features if features is not None else ["hangman"],
        "ttl": ttl,
    }


def build_error(src: str, dst: str, ref_id: str, code: str, message: str) -> dict:
    return {
        "type": "ERROR",
        "msg_id": str(uuid.uuid4()),
        "src": src,
        "dst": dst,
        "ref_msg_id": ref_id,
        "code": code,
        "message": message,
        "ttl": 1,
    }


def build_bye_ok(src: str, msg_id: str, dst: str):
    return {
        "type": "BYE_OK",
        "msg_id": msg_id,
        "src": src,
        "dst": dst,
        "ttl": 1,
    }


def build_bye(src: str, dst: str, reason: str = "Unknown"):
    return {
        "type": "BYE",
        "msg_id": str(uuid.uuid4()),
        "src": src,
        "dst": dst,
        "reason": reason,
        "ttl": 1,
    }


def build_ping():
    return {
        "type": "PING",
        "msg_id": str(uuid.uuid4()),
        "timestamp": str(datetime.now().astimezone()),
        "ttl": 1,
    }


def build_pong(msg_id: str, tstamp: str):
    return {"type": "PONG", "msg_id": msg_id, "timestamp": tstamp, "ttl": 1}
