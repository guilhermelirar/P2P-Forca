import logging

from app import rendezvous_connection as rdv
from app.config_loader import Config, load_config


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

    # TODO migrar para ArgumentParser
    if len(argv) == 3:
        cfg.name = argv[1]
        cfg.listen_port = int(argv[2])

    setup_logging(cfg.log_file)

    while True:
        rdv.register(cfg)
        rdv.discover(cfg.rdv_server, cfg.peer_id)
        if input() == "/quit":
            rdv.unregister(cfg.rdv_server, cfg.namespace, cfg.name, cfg.listen_port)
            return


if __name__ == "__main__":
    main()
