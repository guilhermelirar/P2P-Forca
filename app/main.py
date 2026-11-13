import logging
import threading

import app.peer_connection
from app import rendezvous_connection as rdv
from app.config_loader import Config, load_config
from app.peer_table import PeerTable


def setup_logging(log_file: str | None = None):
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(fmt))

    handlers: list = [console_handler]

    if log_file:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter(fmt))
        handlers.append(file_handler)

    logging.basicConfig(level=logging.DEBUG, handlers=handlers)


def main():  # TODO mover logica para p2p_client.py
    logger = logging.getLogger(__name__)

    from sys import argv

    cfg: Config = load_config()

    if len(argv) == 3:
        cfg.name = argv[1]
        cfg.listen_port = int(argv[2])

    setup_logging(cfg.log_file)

    table = PeerTable(cfg.peer_id)
    stop_event = threading.Event()

    def handle_incoming(
        remote_peer_id: str, conn: app.peer_connection.PeerConnection
    ) -> None:
        logger.info("Conexao INBOUND aceita de %s", remote_peer_id)
        table.attach_inbound(remote_peer_id, conn)

    server_sock = app.peer_connection.start_server(cfg, handle_incoming, stop_event)

    while True:
        my_ip, _ = rdv.register(cfg)

        peers = rdv.discover(cfg.rdv_server, my_ip, cfg.peer_id)
        if peers:
            table.upsert_from_discover(peers)

        for p in table.all_peers():
            if p.can_connect and p.addr and p.conn is None:
                conn = app.peer_connection.connect_to_peer(p.peer_id, p.addr, cfg)
                if conn:
                    table.attach_outbound(p.peer_id, conn)

        if input() == "/quit":
            stop_event.set()
            server_sock.close()
            rdv.unregister(cfg.rdv_server, cfg.namespace, cfg.name, cfg.listen_port)
            return


if __name__ == "__main__":
    main()
