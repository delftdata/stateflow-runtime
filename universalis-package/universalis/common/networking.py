import asyncio
import fractions
import struct
import socket

import zmq
from aiozmq import create_zmq_stream, ZmqStream

from .logging import logging
from .serialization import Serializer, msgpack_serialization, msgpack_deserialization, \
    cloudpickle_serialization, cloudpickle_deserialization


class NetworkingManager:

    def __init__(self):
        self.conns: dict[tuple[str, int], ZmqStream] = {}  # HERE BETTER TO ADD A CONNECTION POOL
        self.locks: dict[tuple[str, int], asyncio.Lock] = {}
        self.host_name: str = str(socket.gethostbyname(socket.gethostname()))
        self.waited_ack_events: dict[int, asyncio.Event] = {}  # event_id: ack_event
        self.ack_id: int = 0
        self.ack_fraction: dict[int, fractions.Fraction] = {}
        self.waited_ack_events_lock: asyncio.Lock = asyncio.Lock()

    def cleanup_after_epoch(self):
        self.waited_ack_events = {}
        self.ack_fraction = {}
        self.ack_id = 0

    def add_ack_fraction_str(self, ack_id: int, fraction_str: str):
        self.ack_fraction[ack_id] += fractions.Fraction(fraction_str)
        if self.ack_fraction[ack_id] == 1:
            # All ACK parts have been gathered
            self.waited_ack_events[ack_id].set()

    def close_all_connections(self):
        for stream in self.conns.values():
            stream.close()
        for lock in self.locks.values():
            lock.release()
        self.conns: dict[tuple[str, int], ZmqStream] = {}
        self.locks: dict[tuple[str, int], asyncio.Lock] = {}

    async def create_socket_connection(self, host: str, port):
        self.locks[(host, port)] = asyncio.Lock()
        self.conns[(host, port)] = await create_zmq_stream(zmq.DEALER, connect=f"tcp://{host}:{port}")

    def close_socket_connection(self, host: str, port):
        if (host, port) in self.conns:
            self.conns[(host, port)].close()
            del self.conns[(host, port)]
            self.locks[(host, port)].release()
            del self.locks[(host, port)]
        else:
            logging.warning('The socket that you are trying to close does not exist')

    async def send_message(self,
                           host,
                           port,
                           msg: dict[str, object],
                           serializer: Serializer = Serializer.CLOUDPICKLE):

        if (host, port) not in self.conns:
            await self.create_socket_connection(host, port)
        msg = self.encode_message(msg, serializer)
        self.conns[(host, port)].write((msg, ))

    async def prepare_function_chain(self, ack_share) -> tuple[str, int, str]:
        async with self.waited_ack_events_lock:
            ack_payload = (self.host_name, self.ack_id, ack_share)
            self.waited_ack_events[self.ack_id] = asyncio.Event()
            self.ack_fraction[self.ack_id] = fractions.Fraction(0)
            self.ack_id += 1
        return ack_payload

    async def send_message_ack(self,
                               host,
                               port,
                               ack_payload: [str, int, str],
                               msg: dict[str, object],
                               serializer: Serializer = Serializer.CLOUDPICKLE):
        msg["__ACK__"] = ack_payload
        await self.send_message(host, port, msg, serializer=serializer)

    async def __receive_message(self, host, port):
        # To be used only by the request response because the lock is needed
        answer = await self.conns[(host, port)].read()
        return self.decode_message(answer[0])

    async def send_message_request_response(self,
                                            host,
                                            port,
                                            msg: dict[str, object],
                                            serializer: Serializer = Serializer.CLOUDPICKLE):

        if (host, port) not in self.conns:
            await self.create_socket_connection(host, port)
        async with self.locks[(host, port)]:
            await self.send_message(host, port, msg, serializer)
            resp = await self.__receive_message(host, port)
            logging.info("NETWORKING MODULE RECEIVED RESPONSE")
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
