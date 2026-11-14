import threading

from app.config_loader import Config
from app.messages import build_bye_ok, build_error
from app.peer_connection import PeerConnection
from app.peer_table import PeerTable


class MessageRouter:
    def __init__(
        self, conf: Config, peer_table: PeerTable, quit_event: threading.Event
    ) -> None:
        self.conf = conf
        self.peer_table = peer_table
        self.quit_event = quit_event

    def handle(self, msg: dict, conn: PeerConnection):
        if not conn.peer_id:
            return
        type = msg.get("type")
        ttl = msg.get("ttl")
        id = msg.get("msg_id")

        # TODO -> implementar ERROR unkown_type
        if type not in ["BYE", "BYE_OK"] or ttl != 1 or id is None:
            return

        # not for me
        if msg.get("dst") != self.conf.peer_id:
            conn.send(
                build_error(
                    src=self.conf.peer_id,
                    dst=conn.peer_id,
                    ref_id=id,
                    code="not_for_me",
                    message="Message not for me",
                )
            )
            return

        match type:
            case "BYE":
                conn.send(
                    build_bye_ok(src=self.conf.peer_id, msg_id=id, dst=conn.peer_id)
                )
                self.peer_table.mark_gone(conn.peer_id)
