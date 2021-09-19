import msgpack
import cloudpickle

from socket import socket, AF_INET, SOCK_STREAM

from common.logging import logging


class NetworkTCPClient:

    def __init__(self,
                 networking_process_address: str = '0.0.0.0',
                 networking_process_port: int = 8888,
                 buffer_size: int = 4096):
        self.buffer_size = buffer_size
        self.networking_process_address = networking_process_address
        self.networking_process_port = networking_process_port

    def send_request_to_other_operator(self, operator_name: str, function_name: str, params):
        payload = {'__OP_NAME__': operator_name, '__FUN_NAME__': function_name, '__PARAMS__': params}
        self.transmit_tcp_no_response(payload, 'REMOTE_FUN_CALL')

    def transmit_tcp_no_response(self, message: object, com_type: str = "NO_RESP") -> None:
        try:
            with socket(AF_INET, SOCK_STREAM) as s:
                s.connect((self.networking_process_address, self.networking_process_port))
                s.setblocking(False)
                if com_type == "NO_RESP":
                    s.sendall(self.msgpack_serialization({"__COM_TYPE__": "NO_RESP", "__MSG__": message}))
                elif com_type == "REMOTE_FUN_CALL":
                    s.sendall(self.msgpack_serialization({"__COM_TYPE__": "REMOTE_FUN_CALL", "__MSG__": message}))
                elif com_type == "SCHEDULE_OPERATOR":
                    s.sendall(cloudpickle.dumps({"__COM_TYPE__": "SCHEDULE_OPERATOR", "__MSG__": message}))
                elif com_type == "REGISTER_OPERATOR_INGRESS":
                    s.sendall(cloudpickle.dumps({"__COM_TYPE__": "REGISTER_OPERATOR_INGRESS", "__MSG__": message}))
                elif com_type == 'RUN_FUN':
                    s.sendall(cloudpickle.dumps({"__COM_TYPE__": "RUN_FUN", "__MSG__": message}))
                else:
                    logging.error(f"Invalid communication type: {com_type}")
        except ConnectionRefusedError:
            logging.error(f"Failed to connect to server: "
                          f"{self.networking_process_address}:{self.networking_process_port}")

    def transmit_tcp_request_response(self, message: object) -> object:
        try:
            with socket(AF_INET, SOCK_STREAM) as s:
                s.connect((self.networking_process_address, self.networking_process_port))
                s.setblocking(False)
                s.sendall(self.msgpack_serialization({"__COM_TYPE__": "REQ_RESP", "__MSG__": message}))
                return self.msgpack_deserialization(self.receive_entire_message_from_socket(s))
        except ConnectionRefusedError:
            logging.error(f"Failed to connect to server "
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
