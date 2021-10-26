import asyncio
from socket import socket, AF_INET, SOCK_STREAM

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
    if com_type in ["NO_RESP", "INVOKE_LOCAL", "REGISTER_OPERATOR_INGRESS"]:
        writer.write(msgpack_serialization({"__COM_TYPE__": com_type, "__MSG__": message}))
    elif com_type in ["RUN_FUN", "RECEIVE_EXE_PLN", "REGISTER_OPERATOR"]:
        writer.write(cloudpickle.dumps({"__COM_TYPE__": com_type, "__MSG__": message}))
    else:
        logging.error(f"Invalid communication type: {com_type}")
    await writer.drain()
    writer.close()
    await writer.wait_closed()


async def async_transmit_tcp_request_response(host: str, port: int, message: object):
    reader, writer = await asyncio.open_connection(host, port)
    logging.debug(f"REQ RESP Target machine: {host}:{port} message: {message}")
    writer.write(msgpack_serialization({"__COM_TYPE__": "REQ_RESP", "__MSG__": message}))
    data = await reader.read()
    writer.close()
    await writer.wait_closed()
    return msgpack_deserialization(data)
