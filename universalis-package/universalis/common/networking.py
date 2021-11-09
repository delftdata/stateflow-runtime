import asyncio
from socket import socket, AF_INET, SOCK_STREAM
from websockets import connect

import cloudpickle

from universalis.common.logging import logging
from universalis.common.serialization import msgpack_serialization, msgpack_deserialization

BUFFER_SIZE = 4096  # in bytes


def receive_entire_message_from_socket(s: socket) -> bytes:
    fragments = []
    while True:
        chunk = s.recv(BUFFER_SIZE)
        if not chunk:
            break
        fragments.append(chunk)
    return b''.join(fragments)


def transmit_tcp_request_response(host: str, port: int, message: object) -> object:
    try:
        with socket(AF_INET, SOCK_STREAM) as s:
            s.connect((host, port))
            s.setblocking(False)
            logging.debug(f"REQ RESP Target machine: {host}:{port} message: {message}")
            s.sendall(msgpack_serialization({"__COM_TYPE__": "REQ_RESP", "__MSG__": message}))
            return msgpack_deserialization(receive_entire_message_from_socket(s))
    except ConnectionRefusedError:
        logging.error(f"Failed to connect to server {host}:{port}")


def transmit_tcp_no_response(host: str, port: int, message: object, com_type: str = "NO_RESP") -> None:
    try:
        with socket(AF_INET, SOCK_STREAM) as s:
            s.connect((host, port))
            s.setblocking(False)
            if com_type == "NO_RESP":
                logging.debug(f"NO RESP Target machine: {host}:{port} message: {message}")
                s.sendall(msgpack_serialization({"__COM_TYPE__": "NO_RESP", "__MSG__": message}))
            elif com_type == "GET_PEERS":
                s.sendall(msgpack_serialization({"__COM_TYPE__": "GET_PEERS", "__MSG__": message}))
            elif com_type == "INVOKE_LOCAL":
                s.sendall(msgpack_serialization({"__COM_TYPE__": "INVOKE_LOCAL", "__MSG__": message}))
            elif com_type == "RUN_FUN":
                s.sendall(cloudpickle.dumps({"__COM_TYPE__": "RUN_FUN", "__MSG__": message}))
            elif com_type == "RECEIVE_EXE_PLN":
                message: bytes
                s.sendall(cloudpickle.dumps({"__COM_TYPE__": "RECEIVE_EXE_PLN", "__MSG__": message}))
            elif com_type == "REGISTER_OPERATOR_INGRESS":
                s.sendall(msgpack_serialization({"__COM_TYPE__": "REGISTER_OPERATOR_INGRESS", "__MSG__": message}))
            elif com_type == "REGISTER_OPERATOR":
                s.sendall(cloudpickle.dumps({"__COM_TYPE__": "REGISTER_OPERATOR", "__MSG__": message}))
            else:
                logging.error(f"Invalid communication type: {com_type}")
    except ConnectionRefusedError:
        logging.error(f"Failed to connect to server {host}:{port}")


async def async_transmit_tcp_no_response(host: str, port: int, message: object, com_type: str = "NO_RESP") -> None:
    _, writer = await asyncio.open_connection(host, port)
    if com_type in ["NO_RESP", "INVOKE_LOCAL", "REGISTER_OPERATOR_DISCOVERY", "REMOTE_FUN_CALL"]:
        writer.write(msgpack_serialization({"__COM_TYPE__": com_type, "__MSG__": message}))
    elif com_type in ["RUN_FUN", "RECEIVE_EXE_PLN", "REGISTER_OPERATOR"]:
        writer.write(cloudpickle.dumps({"__COM_TYPE__": com_type, "__MSG__": message}))
    else:
        logging.error(f"Invalid communication type: {com_type}")
    await writer.drain()
    writer.close()
    await writer.wait_closed()


async def async_transmit_tcp_request_response(host: str, port: int, message: object, com_type: str = "REQ_RESP"):
    reader, writer = await asyncio.open_connection(host, port)
    logging.debug(f"REQ RESP Target machine: {host}:{port} message: {message}")
    writer.write(msgpack_serialization({"__COM_TYPE__": com_type, "__MSG__": message}))
    await writer.drain()
    writer.write_eof()
    data = await reader.read()
    writer.close()
    await writer.wait_closed()
    return msgpack_deserialization(data)


async def async_transmit_websocket_no_response_new_connection(host: str,
                                                              port: int,
                                                              message: object,
                                                              com_type: str = "NO_RESP"):
    async with connect(f"ws://{host}:{port}") as websocket:
        if com_type in ["NO_RESP", "INVOKE_LOCAL", "REGISTER_OPERATOR_DISCOVERY", "REMOTE_FUN_CALL"]:
            serialized_message = msgpack_serialization({"__COM_TYPE__": com_type, "__MSG__": message})
        elif com_type in ["RUN_FUN", "RECEIVE_EXE_PLN", "REGISTER_OPERATOR"]:
            serialized_message = cloudpickle.dumps({"__COM_TYPE__": com_type, "__MSG__": message})
            logging.error(f"Invalid communication type: {com_type}")
        await websocket.send(serialized_message)


async def async_transmit_websocket_request_response_new_connection(host: str,
                                                                   port: int,
                                                                   message: object,
                                                                   com_type: str = "REQ_RESP"):
    async with connect(f"ws://{host}:{port}") as websocket:
        serialized_message = msgpack_serialization({"__COM_TYPE__": com_type, "__MSG__": message})
        await websocket.send(serialized_message)
        return msgpack_deserialization(await websocket.recv())


class UniversalisClientProtocol(asyncio.Protocol):
    def __init__(self, message, on_con_lost):
        self.message = message
        self.on_con_lost = on_con_lost

    def connection_made(self, transport):
        request = self.message.encode()
        transport.write(request)
        print('Data sent: {!r}'.format(self.message))

    def data_received(self, data):
        response = data.decode()

    def connection_lost(self, exc):
        print('The server closed the connection')
        self.on_con_lost.set_result(True)
