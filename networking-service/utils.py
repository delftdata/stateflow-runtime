import asyncio

import msgpack
import logging
from socket import socket, AF_INET, SOCK_STREAM
from timeit import default_timer as timer


BUFFER_SIZE = 4096  # in bytes


def msgpack_serialization(serializable_object: object) -> bytes:
    return msgpack.packb(serializable_object)


def msgpack_deserialization(serialized_object: bytes) -> dict:
    return msgpack.unpackb(serialized_object)


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
            else:
                logging.error(f"Invalid communication type: {com_type}")
    except ConnectionRefusedError:
        logging.error(f"Failed to connect to server {host}:{port}")


async def async_transmit_tcp_no_response(host: str, port: int, message: object, com_type: str = "NO_RESP") -> None:
    reader, writer = await asyncio.open_connection(host, port)
    if com_type == "NO_RESP":
        logging.debug(f"Target machine: {host}:{port} message: {message}")
        writer.write(msgpack_serialization({"__COM_TYPE__": "NO_RESP", "__MSG__": message}))
    elif com_type == "GET_PEERS":
        writer.write(msgpack_serialization({"__COM_TYPE__": "GET_PEERS", "__MSG__": message}))
    else:
        logging.error(f"Invalid communication type: {com_type}")
    await writer.drain()
    writer.close()


async def async_transmit_tcp_request_response(host: str, port: int, message: object) -> object:
    reader, writer = await asyncio.open_connection(host, port)
    logging.debug(f"REQ RESP Target machine: {host}:{port} message: {message}")
    writer.write(msgpack_serialization({"__COM_TYPE__": "REQ_RESP", "__MSG__": message}))
    data = await reader.read()
    writer.close()
    return msgpack_deserialization(data)


async def benchmark_peers(peers: list[tuple[str, int]]):

    msg = {"1": [1, 2, 3], "2": [4, 5, 6], "3": [7, 8, 9], "4": [1, 2, 3], "5": [4, 5, 6], "6": [7, 8, 9],
           "7": [1, 2, 3], "8": [4, 5, 6], "9": [7, 8, 9]}

    n_runs = 10000

    for host, port in peers:

        logging.info(f"------------- RUN AGAINST {host}:{port} -------------")
        logging.info("STARTING ONE WAY COMMUNICATION TESTS")

        total_runtime = 0
        for _ in range(n_runs):
            start = timer()
            await async_transmit_tcp_no_response(host, port, msg)
            end = timer()
            runtime = end - start
            total_runtime += runtime

        avg_runtime = total_runtime / n_runs
        logging.info(f"No Response total runtime on {n_runs} runs is {total_runtime} seconds")
        logging.info("No Response: %.3f ms on average" % (1000 * avg_runtime))

        logging.info("STARTING REQUEST/RESPONSE COMMUNICATION TESTS")

        total_runtime = 0
        for _ in range(n_runs):
            start = timer()
            await async_transmit_tcp_request_response(host, port, msg)
            end = timer()
            runtime = end - start
            total_runtime += runtime

        avg_runtime = total_runtime / n_runs

        logging.info(f"Request/Response total runtime on {n_runs} runs is {total_runtime} seconds")
        logging.info("Request/Response: %.3f ms on average" % (1000 * avg_runtime))
