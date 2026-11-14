import logging
import threading
import time

import app.peer_connection
import app.rendezvous_connection as rdv
from app.config_loader import Config
from app.message_router import MessageRouter
from app.messages import build_bye
from app.peer_table import PeerTable

logger = logging.getLogger(__name__)


class P2PClient:
    def __init__(self, config: Config) -> None:
        self.conf = config
        self.peer_table = PeerTable(self.conf.peer_id)
        self.quit_event = threading.Event()
        self.pub_ip = "0.0.0.0"
        self.server_sock = app.peer_connection.start_server(
            self.conf, self.handle_incoming, self.quit_event
        )
        self.router = MessageRouter(self.conf, self.peer_table, self.quit_event)

    def handle_incoming(
        self, remote_peer_id: str, conn: app.peer_connection.PeerConnection
    ):
        logger.info("Conexao INBOUND aceita de %s", remote_peer_id)
        if self.peer_table.attach_connection(remote_peer_id, conn):
            threading.Thread(target=self._read_loop, args=(conn,), daemon=True).start()

    def _read_loop(self, conn: app.peer_connection.PeerConnection):
        while not conn.closed:
            msg = conn.recv_json()
            if msg is None:
                # SOCKET closed (MARCAR COMO STALE)
                break

            self.router.handle(msg, conn)

    def _register_loop(self):
        logger.debug("[P2PClient] client's register loop thread started")
        interval = self.conf.register_ttl
        while not self.quit_event.is_set():
            self.quit_event.wait(interval)
            resp = rdv.register(self.conf)
            if resp:
                _, ttl = resp
                interval = min(ttl * 0.75, 300)

    def _discover_loop(self, my_ip: str):
        """
        Executa o DISCOVER a cada discover_interval definido pela configuração,
        atualiza a tabela de peers e inicia as conexões com os novos peers descobertos.
        Aborta quando quit_event é setado.
        """
        logger.debug("[P2PClient] client's discover loop thread started")
        while not self.quit_event.is_set():
            peers = rdv.discover(self.conf.rdv_server, my_ip, self.conf.peer_id)

            # se há peers, atualizar tabela e criar novas conexões
            # caso contrário, lança warning
            if peers is not None:
                self.peer_table.upsert_from_discover(peers)

                for p in self.peer_table.all_peers():
                    if p.state == "DISCOVERED" and p.addr and p.conn is None:
                        conn = app.peer_connection.connect_to_peer(
                            p.peer_id, p.addr, self.conf
                        )
                        if conn and self.peer_table.attach_connection(p.peer_id, conn):
                            threading.Thread(
                                target=self._read_loop, args=(conn,), daemon=True
                            ).start()

            else:
                logger.warning(
                    f"[RDV] DISCOVER fail, attempting again in {self.conf.discover_interval} s"
                )

            # espera discover_interval ou evento de quit
            self.quit_event.wait(self.conf.discover_interval)

    def start(self):
        rsp = rdv.register(self.conf)
        if not rsp:
            logger.error("[P2PClient] could not start")
            return

        my_ip, self.conf.register_ttl = rsp

        threading.Thread(target=self._register_loop, daemon=True).start()
        threading.Thread(target=self._discover_loop, args=(my_ip,), daemon=True).start()

        logger.debug("[P2PClient] P2PClient.start() was called")

    def stop(self):
        self.quit_event.set()
        peers = self.peer_table.all_peers()

        # Envia BYE e espera 1s para receber BYE_OK
        for p in peers:
            conn = p.conn
            if p.state == "CONNECTED" and conn is not None and not conn.closed:
                conn.send(
                    build_bye(src=self.conf.peer_id, dst=p.peer_id, reason="shutdown")
                )

        time.sleep(1.0)
        for p in peers:
            conn = p.conn
            if p.state == "CONNECTED" and conn is not None and not conn.closed:
                conn.close()

        self.server_sock.close()
        rdv.unregister(
            self.conf.rdv_server,
            self.conf.namespace,
            self.conf.name,
            self.conf.listen_port,
        )
