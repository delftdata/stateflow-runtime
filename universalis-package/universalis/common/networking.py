import struct
import socket
import cloudpickle

from .logging import logging
from .serialization import Serializer, msgpack_deserialization, msgpack_serialization


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

    def send_message(self, host, port, msg: object, serializer: Serializer = Serializer.CLOUDPICKLE):
        sock = self.open_socket_connections[(host, port)]
        msg = encode_message(msg, serializer)
        try:
            sock.sendall(msg)
        except BrokenPipeError:
            self.close_socket_connection(host, port)
            self.create_socket_connection(host, port)
            self.open_socket_connections[(host, port)].sendall(msg)

    def receive_message(self, host, port):
        sock = self.open_socket_connections[(host, port)]
        # Read message length and unpack it into an integer
        raw_serializer = self.__receive_all(sock, 2)
        raw_message_len = self.__receive_all(sock, 4)
        if not raw_message_len or not raw_serializer:
            return None
        serializer = struct.unpack('>H', raw_serializer)[0]
        message_len = struct.unpack('>I', raw_message_len)[0]
        # Read the message data
        if serializer == 0:
            return cloudpickle.loads(self.__receive_all(sock, message_len))
        elif serializer == 1:
            return msgpack_deserialization(self.__receive_all(sock, message_len))
        else:
            logging.error(f'Serializer is not supported')

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


def encode_message(msg: object, serializer: Serializer):
    if serializer == Serializer.CLOUDPICKLE:
        msg = cloudpickle.dumps(msg)
        msg = struct.pack('>H', 0) + struct.pack('>I', len(msg)) + msg
        return msg
    elif serializer == Serializer.MSGPACK:
        msg = msgpack_serialization(msg)
        msg = struct.pack('>H', 1) + struct.pack('>I', len(msg)) + msg
        return msg
    else:
        logging.error(f'Serializer: {serializer} is not supported')
        return None


def decode_messages(data):
    while 1:
        serializer = struct.unpack('>H', data[0:2])[0]
        message_len = struct.unpack('>I', data[2:6])[0]
        packet_len = message_len + 6
        msg = data[6:packet_len]
        if serializer == 0:
            yield cloudpickle.loads(msg)
        elif serializer == 1:
            yield msgpack_deserialization(msg)
        else:
            logging.error(f'Serializer is not supported')
        data = data[packet_len:]
        if len(data) == 0:
            break
