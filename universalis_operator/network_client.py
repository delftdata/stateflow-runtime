import logging
import msgpack
from socket import socket, AF_INET, SOCK_STREAM


class NetworkTCPClient:

    def __init__(self, buffer_size: int = 4096):
        self.buffer_size = buffer_size
        self.networking_process_address = '0.0.0.0'
        self.networking_process_port = 8888

    def transmit_tcp_request_response(self, message: object) -> object:
        try:
            with socket(AF_INET, SOCK_STREAM) as s:
                s.connect((self.networking_process_address, self.networking_process_port))
                s.setblocking(False)
                logging.debug(f"REQ RESP Target machine: "
                              f"{self.networking_process_address}:{self.networking_process_port} message: {message}")
                s.sendall(self.msgpack_serialization({"__COM_TYPE__": "REQ_RESP", "__MSG__": message}))
                return self.msgpack_deserialization(self.receive_entire_message_from_socket(s))
        except ConnectionRefusedError:
            logging.error(f"Failed to connect to server "
                          f"{self.networking_process_address}:{self.networking_process_port}")

    def transmit_tcp_no_response(self, message: object, com_type: str = "NO_RESP") -> None:
        try:
            with socket(AF_INET, SOCK_STREAM) as s:
                s.connect((self.networking_process_address, self.networking_process_port))
                s.setblocking(False)
                if com_type == "NO_RESP":
                    logging.debug(f"NO RESP Target machine: "
                                  f"{self.networking_process_address}:{self.networking_process_port} message: {message}")
                    s.sendall(self.msgpack_serialization({"__COM_TYPE__": "NO_RESP", "__MSG__": message}))
                elif com_type == "GET_PEERS":
                    s.sendall(self.msgpack_serialization({"__COM_TYPE__": "GET_PEERS", "__MSG__": message}))
                else:
                    logging.error(f"Invalid communication type: {com_type}")
        except ConnectionRefusedError:
            logging.error(f"Failed to connect to server: "
                          f"{self.networking_process_address}:{self.networking_process_port}")

    def receive_entire_message_from_socket(self, s: socket) -> bytes:
        fragments = []
        while True:
            chunk = s.recv(self.buffer_size)
            if not chunk:
                break
            fragments.append(chunk)
        return b''.join(fragments)

    @staticmethod
    def msgpack_serialization(serializable_object: object) -> bytes:
        return msgpack.packb(serializable_object)

    @staticmethod
    def msgpack_deserialization(serialized_object: bytes) -> dict:
        return msgpack.unpackb(serialized_object)
