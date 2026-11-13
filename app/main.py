import logging
import threading

import app.peer_connection
from app import rendezvous_connection as rdv
from app.config_loader import Config, load_config
from app.p2p_client import P2PClient
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


def main():
    from sys import argv

    cfg: Config = load_config()

    if len(argv) == 3:
        cfg.name = argv[1]
        cfg.listen_port = int(argv[2])

    setup_logging(cfg.log_file)

    client = P2PClient(cfg)
    client.start()


if __name__ == "__main__":
    main()
