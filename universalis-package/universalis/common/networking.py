import struct
import socket
import cloudpickle

from .logging import logging
from .serialization import Serializer, msgpack_deserialization, msgpack_serialization


class NetworkingManager:

    def __init__(self):
        self.open_socket_connections: dict[tuple[str, int, str, str], socket.socket] = {}

    @staticmethod
    def create_socket_connection(host: str, port) -> socket.socket:
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
            return s

    def close_socket_connection(self, host: str, port: int, operator_name: str, function_name: str):

        if (host, port, operator_name, function_name) in self.open_socket_connections:
            self.open_socket_connections[(host, port, operator_name, function_name)].close()
            del self.open_socket_connections[(host, port, operator_name, function_name)]
        else:
            logging.warning('The socket that you are trying to close does not exist')

    def send_message(self,
                     host,
                     port,
                     operator_name,
                     function_name,
                     msg: object,
                     serializer: Serializer = Serializer.CLOUDPICKLE,
                     tmp_socket: socket.socket = None):
        if tmp_socket is None:
            if (host, port, operator_name, function_name) not in self.open_socket_connections:
                self.open_socket_connections[(host, port, operator_name, function_name)] = self.create_socket_connection(host, port)
            sock = self.open_socket_connections[(host, port, operator_name, function_name)]
        else:
            sock = tmp_socket
        msg = encode_message(msg, serializer)
        try:
            sock.sendall(msg)
        except BrokenPipeError:
            if tmp_socket is None:
                self.close_socket_connection(host, port, operator_name, function_name)
                self.open_socket_connections[(host, port, operator_name, function_name)] = self.create_socket_connection(host, port)
                self.open_socket_connections[(host, port, operator_name, function_name)].sendall(msg)
            else:
                tmp_socket.close()
                sock = self.create_socket_connection(host, port)
                sock.sendall(msg)

    def receive_message(self, host, port, operator_name, function_name, tmp_socket: socket.socket = None):

        # logging.warning(f'(N)  RCV M -> 1')
        if tmp_socket is None:
            sock = self.open_socket_connections[(host, port, operator_name, function_name)]
        else:
            sock = tmp_socket
        # Read message length and unpack it into an integer
        raw_serializer = self.__receive_all(sock, 2)
        # logging.warning(f'(N)  RCV M -> 2')
        raw_message_len = self.__receive_all(sock, 4)
        # logging.warning(f'(N)  RCV M -> 3')
        if not raw_message_len or not raw_serializer:
            return None
        serializer = struct.unpack('>H', raw_serializer)[0]
        message_len = struct.unpack('>I', raw_message_len)[0]
        # Read the message data
        if serializer == 0:
            # logging.warning(f'(N)  RCV M -> 4')
            return cloudpickle.loads(self.__receive_all(sock, message_len))
        elif serializer == 1:
            # logging.warning(f'(N)  RCV M -> 4')
            return msgpack_deserialization(self.__receive_all(sock, message_len))
        else:
            logging.error(f'Serializer is not supported')

    def send_message_request_response(self,
                                      host,
                                      port,
                                      operator_name,
                                      function_name,
                                      msg: object,
                                      serializer: Serializer = Serializer.CLOUDPICKLE):
        logging.warning(msg)
        logging.warning(f'(N) -> 1')
        self.send_message(host, port, operator_name, function_name, msg, serializer=serializer)
        logging.warning(f'(N) -> 2')
        response = self.receive_message(host, port, operator_name, function_name)
        logging.warning(f'(N) -> 3')
        return response

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
