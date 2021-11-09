import asyncio
import os
import socket
import struct
import cloudpickle
import uvloop

from universalis.common.logging import logging
from universalis.common.serialization import msgpack_deserialization, msgpack_serialization
from universalis.common.stateflow_worker import StateflowWorker
from universalis.common.stateful_function import make_key_hashable, StrKeyNotUUID, NonSupportedKeyType

SERVER_PORT = 8888
DISCOVERY_HOST = os.environ['DISCOVERY_HOST']
DISCOVERY_PORT = int(os.environ['DISCOVERY_PORT'])


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


class IngressServerProtocol(asyncio.Protocol):

    def __init__(self):
        self.transport: asyncio.Transport = asyncio.Transport()

    def connection_made(self, transport: asyncio.Transport):
        peername = transport.get_extra_info('peername')
        logging.info(f"Connection from {peername}")
        self.transport = transport

    def data_received(self, data):
        global registered_operator_connections
        deserialized_data = msgpack_deserialization(data[4:])
        if '__COM_TYPE__' not in deserialized_data:
            logging.error(f"Deserialized data do not contain a message type")
        else:
            message_type: str = deserialized_data['__COM_TYPE__']
            message: dict = deserialized_data['__MSG__']
            if message_type == 'REMOTE_FUN_CALL':
                # RECEIVE MESSAGE FROM A CLIENT TO PASS INTO A STATEFLOW GRAPH'S OPERATOR FUNCTION
                operator_name = message['__OP_NAME__']
                key = message['__KEY__']

                if operator_name not in registered_operator_connections:
                    self.__get_registered_operators()
                try:
                    try:
                        partition: str = str(int(make_key_hashable(key)) %
                                             len(registered_operator_connections[operator_name].keys()))
                        worker: tuple[str, int] = registered_operator_connections[operator_name][partition]
                    except KeyError:
                        self.__get_registered_operators()
                        logging.info(registered_operator_connections)
                        partition: str = str(
                            int(make_key_hashable(key)) % len(registered_operator_connections[operator_name].keys()))
                        worker: tuple[str, int] = registered_operator_connections[operator_name][partition]
                    worker: StateflowWorker = StateflowWorker(worker[0], worker[1])
                    logging.debug(f"Opening connection to: {worker.host}:{worker.port}")

                    if (worker.host, worker.port) not in networking_manager.open_socket_connections:
                        networking_manager.create_socket_connection(worker.host, worker.port)
                    message.update({'__PARTITION__': int(partition)})
                    logging.debug(f'Sending packet: {message} to {worker.host}:{worker.port}')
                    networking_manager.send_message(worker.host,
                                                    worker.port,
                                                    cloudpickle.dumps({"__COM_TYPE__": "RUN_FUN",
                                                                       "__MSG__": message}))
                except StrKeyNotUUID:
                    logging.error(f"String key: {key} is not a UUID")
                except NonSupportedKeyType:
                    logging.error(f"Supported keys are integers and UUIDS not {type(key)}")
            else:
                logging.error(f"INGRESS SERVER: Non supported message type: {message_type}")

    @staticmethod
    def __get_registered_operators():
        global registered_operator_connections
        networking_manager.send_message(DISCOVERY_HOST,
                                        DISCOVERY_PORT,
                                        msgpack_serialization({"__COM_TYPE__": "DISCOVER",
                                                               "__MSG__": ""}))
        registered_operator_connections = msgpack_deserialization(networking_manager.receive_message(DISCOVERY_HOST,
                                                                                                     DISCOVERY_PORT))


async def main():
    loop = asyncio.get_running_loop()
    server = await loop.create_server(lambda: IngressServerProtocol(), '0.0.0.0', SERVER_PORT)
    logging.info(f"Ingress Server listening at 0.0.0.0:{SERVER_PORT}")

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    registered_operator_connections: dict[dict[str, tuple[str, int]]] = {}
    networking_manager: NetworkingManager = NetworkingManager()
    networking_manager.create_socket_connection(DISCOVERY_HOST, DISCOVERY_PORT)
    uvloop.install()
    asyncio.run(main())
