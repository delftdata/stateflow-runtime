import asyncio
import struct

import zmq
from aiozmq import create_zmq_stream, ZmqStream

from .logging import logging
from .serialization import Serializer, msgpack_serialization, msgpack_deserialization, \
    cloudpickle_serialization, cloudpickle_deserialization


class NetworkingManager:

    def __init__(self):
        self.conns: dict[tuple[str, int, str, str], ZmqStream] = {}
        self.locks: dict[tuple[str, int, str, str], asyncio.Lock] = {}

    def close_all_connections(self):
        for stream in self.conns.values():
            stream.close()
        for lock in self.locks.values():
            lock.release()
        self.conns: dict[tuple[str, int, str, str], ZmqStream] = {}
        self.locks: dict[tuple[str, int, str, str], asyncio.Lock] = {}

    async def create_socket_connection(self, host: str, port, operator_name: str, function_name: str):
        self.locks[(host, port, operator_name, function_name)] = asyncio.Lock()
        self.conns[(host, port, operator_name, function_name)] = await create_zmq_stream(zmq.DEALER,
                                                                                         connect=f"tcp://{host}:{port}")

    def close_socket_connection(self, host: str, port, operator_name: str, function_name: str):
        if (host, port, operator_name, function_name) in self.conns:
            self.conns[(host, port, operator_name, function_name)].close()
            del self.conns[(host, port, operator_name, function_name)]
            self.locks[(host, port, operator_name, function_name)].release()
            del self.locks[(host, port, operator_name, function_name)]
        else:
            logging.warning('The socket that you are trying to close does not exist')

    async def send_message(self,
                           host,
                           port,
                           operator_name,
                           function_name,
                           msg: object,
                           serializer: Serializer = Serializer.CLOUDPICKLE):

        if (host, port, operator_name, function_name) not in self.conns:
            await self.create_socket_connection(host, port, operator_name, function_name)
        msg = self.encode_message(msg, serializer)
        self.conns[(host, port, operator_name, function_name)].write((msg, ))

    async def __receive_message(self, host, port, operator_name, function_name):
        # To be used only by the request response because the lock is needed
        answer = await self.conns[(host, port, operator_name, function_name)].read()
        return self.decode_message(answer[0])

    async def send_message_request_response(self,
                                            host,
                                            port,
                                            operator_name,
                                            function_name,
                                            msg: object,
                                            serializer: Serializer = Serializer.CLOUDPICKLE):

        if (host, port, operator_name, function_name) not in self.conns:
            await self.create_socket_connection(host, port, operator_name, function_name)
        async with self.locks[(host, port, operator_name, function_name)]:
            await self.send_message(host, port, operator_name, function_name, msg, serializer)
            resp = await self.__receive_message(host, port, operator_name, function_name)
            return resp

    @staticmethod
    def encode_message(msg: object, serializer: Serializer) -> bytes:
        if serializer == Serializer.CLOUDPICKLE:
            msg = struct.pack('>H', 0) + cloudpickle_serialization(msg)
            return msg
        elif serializer == Serializer.MSGPACK:
            msg = struct.pack('>H', 1) + msgpack_serialization(msg)
            return msg
        else:
            logging.info(f'Serializer: {serializer} is not supported')

    @staticmethod
    def decode_message(data):
        serializer = struct.unpack('>H', data[0:2])[0]
        if serializer == 0:
            return cloudpickle_deserialization(data[2:])
        elif serializer == 1:
            return msgpack_deserialization(data[2:])
        else:
            logging.info(f'Serializer: {serializer} is not supported')
