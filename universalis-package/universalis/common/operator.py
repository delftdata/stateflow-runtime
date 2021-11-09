import socket
import struct
from typing import Union, Awaitable

from .logging import logging
from .base_operator import BaseOperator
from .function import Function
from .stateful_function import StatefulFunction
from .local_state_backends import LocalStateBackend


class NotAFunctionError(Exception):
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


class Operator(BaseOperator):
    # each operator is a coroutine? and a server. Client is already implemented in the networking module
    def __init__(self,
                 name: str,
                 partitions: int = 1,
                 operator_state_backend: LocalStateBackend = LocalStateBackend.DICT):
        super().__init__(name)
        self.state = None
        self.operator_state_backend: LocalStateBackend = operator_state_backend
        self.functions: dict[str, Union[Function, StatefulFunction]] = {}
        self.dns = {}  # where the other functions exist
        self.partitions = partitions
        self.networking = NetworkingManager()

    async def run_function(self, function_name: str, *params) -> Awaitable:
        logging.info(f'PROCESSING FUNCTION -> {function_name} of operator: {self.name} with params: {params}')
        return await self.functions[function_name](*params)

    def register_function(self, function: Function):
        self.functions[function.name] = function

    def register_stateful_function(self, function: StatefulFunction):
        if StatefulFunction not in type(function).__bases__:
            raise NotAFunctionError
        stateful_function = function
        self.functions[stateful_function.name] = stateful_function

    def attach_state_to_functions(self, state):
        self.state = state
        for function in self.functions.values():
            function.attach_state(self.state)
            function.attach_networking(self.networking)

    def register_stateful_functions(self, *functions: StatefulFunction):
        for function in functions:
            self.register_stateful_function(function)

    def set_function_timestamp(self, fun_name: str, timestamp: int):
        self.functions[fun_name].set_timestamp(timestamp)
