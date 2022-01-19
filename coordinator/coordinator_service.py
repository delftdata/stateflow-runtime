import asyncio

import aiojobs
import aiozmq
import uvloop
import zmq

from universalis.common.logging import logging
from universalis.common.networking import NetworkingManager
from universalis.common.serialization import Serializer

from coordinator import Coordinator

SERVER_PORT = 8888
operator_partition_locations: dict[dict[str, tuple[str, int]]] = {}


async def schedule_operators(networking, message):
    global operator_partition_locations
    coordinator = Coordinator()
    operator_partition_locations = await coordinator.submit_stateflow_graph(networking,
                                                                            message)


async def main():
    global operator_partition_locations
    router = await aiozmq.create_zmq_stream(zmq.ROUTER, bind=f"tcp://0.0.0.0:{SERVER_PORT}")  # coordinator
    scheduler = await aiojobs.create_scheduler(limit=None)
    networking = NetworkingManager()
    logging.info(f"Coordinator Server listening at 0.0.0.0:{SERVER_PORT}")
    while True:
        resp_adr, data = await router.read()
        deserialized_data: dict = networking.decode_message(data)
        if '__COM_TYPE__' not in deserialized_data:
            logging.error(f"Deserialized data do not contain a message type")
        else:
            message_type: str = deserialized_data['__COM_TYPE__']
            message = deserialized_data['__MSG__']
            if message_type == 'SEND_EXECUTION_GRAPH':
                # Received execution graph from a universalis client
                await scheduler.spawn(schedule_operators(networking, message))
                logging.info(f"Submitted Stateflow Graph to Workers")
            elif message_type == 'DISCOVER':
                logging.info(f"DISCOVERY REQUEST RECEIVED")
                reply = networking.encode_message(operator_partition_locations, Serializer.MSGPACK)
                router.write((resp_adr, reply))
                logging.info(f"DISCOVERY RESPONSE {reply}")
            else:
                logging.error(f"COORDINATOR SERVER: Non supported message type: {message_type}")

if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
