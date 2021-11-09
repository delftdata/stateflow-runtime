import socket
import struct

from scheduler.round_robin import RoundRobin

from universalis.common.stateflow_graph import StateflowGraph
from universalis.common.stateflow_worker import StateflowWorker
from universalis.common.logging import logging

class NotAStateflowGraph(Exception):
    pass


class NetworkingManager:

    def __init__(self):
        self.open_socket_connections: dict[tuple[str, int], socket.socket] = {}

    def create_socket_connection(self, host: str, port):
        s = None
        for res in socket.getaddrinfo(host, port, socket.AF_UNSPEC, socket.SOCK_STREAM):
            af, socktype, proto, canonname, sa = res
            try:
                s = socket.socket(af, socktype, proto)
            except OSError:
                s = None
                continue
            try:
                s.connect(sa)
            except OSError:
                s.close()
                s = None
                continue
            break
        if s is None:
            logging.error(f'Could not open socket for host: {host}:{port}')
        else:
            self.open_socket_connections[(host, port)] = s

    def close_socket_connection(self, host: str, port: int):
        if (host, port) in self.open_socket_connections:
            self.open_socket_connections[(host, port)].close()

    def send_message(self, host, port, msg: bytes):
        sock = self.open_socket_connections[(host, port)]
        # Prefix each message with a 4-byte length (network byte order)
        msg = struct.pack('>I', len(msg)) + msg
        sock.sendall(msg)

    def receive_message(self, host, port):
        sock = self.open_socket_connections[(host, port)]
        # Read message length and unpack it into an integer
        raw_message_len = self.__receive_all(sock, 4)
        if not raw_message_len:
            return None
        message_len = struct.unpack('>I', raw_message_len)[0]
        # Read the message data
        return self.__receive_all(sock, message_len)

    @staticmethod
    def __receive_all(sock, n):
        # Helper function to receive n bytes or return None if EOF is hit
        data = bytearray()
        while len(data) < n:
            packet = sock.recv(n - len(data))
            if not packet:
                return None
            data.extend(packet)
        return data


class Coordinator:

    def __init__(self):
        # TODO get workers and ingresses dynamically
        self.workers = [StateflowWorker("worker-0", 8888),
                        StateflowWorker("worker-1", 8888),
                        StateflowWorker("worker-2", 8888)]
        self.network_manager = NetworkingManager()

    async def submit_stateflow_graph(self, stateflow_graph: StateflowGraph, scheduler_type=None):
        if not isinstance(stateflow_graph, StateflowGraph):
            raise NotAStateflowGraph
        scheduler = RoundRobin()
        await scheduler.schedule(self.workers, stateflow_graph)

    async def submit_stateflow_graph_websockets(self, stateflow_graph: StateflowGraph, scheduler_type=None):
        if not isinstance(stateflow_graph, StateflowGraph):
            raise NotAStateflowGraph
        scheduler = RoundRobin()
        await scheduler.schedule_websockets(self.workers, stateflow_graph)

    def submit_stateflow_protocol(self, stateflow_graph: StateflowGraph):
        if not isinstance(stateflow_graph, StateflowGraph):
            raise NotAStateflowGraph
        scheduler = RoundRobin()
        scheduler.schedule_protocol(self.workers, stateflow_graph, self.network_manager)
