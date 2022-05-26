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


class CoordinatorService:

    def __init__(self):
        self.networking = NetworkingManager()
        self.coordinator = Coordinator()
        self.scheduler = None

    async def schedule_operators(self, message):
        await self.coordinator.submit_stateflow_graph(self.networking, message)

    async def main(self):
        router = await aiozmq.create_zmq_stream(zmq.ROUTER, bind=f"tcp://0.0.0.0:{SERVER_PORT}")  # coordinator
        self.scheduler = await aiojobs.create_scheduler(limit=None)
        logging.info(f"Coordinator Server listening at 0.0.0.0:{SERVER_PORT}")
        while True:
            resp_adr, data = await router.read()
            deserialized_data: dict = self.networking.decode_message(data)
            if '__COM_TYPE__' not in deserialized_data:
                logging.error(f"Deserialized data do not contain a message type")
            else:
                message_type: str = deserialized_data['__COM_TYPE__']
                message = deserialized_data['__MSG__']
                match message_type:
                    case 'SEND_EXECUTION_GRAPH':
                        # Received execution graph from a universalis client
                        await self.scheduler.spawn(self.schedule_operators(message))
                        logging.info(f"Submitted Stateflow Graph to Workers")
                    case 'REGISTER_WORKER':
                        # A worker registered to the coordinator
                        reply = self.networking.encode_message(self.coordinator.register_worker(message),
                                                               Serializer.MSGPACK)  # reply = the id given to the worker
                        router.write((resp_adr, reply))
                        logging.info(f"Worker registered {message} with id {reply}")
                    case _:
                        # Any other message type
                        logging.error(f"COORDINATOR SERVER: Non supported message type: {message_type}")


if __name__ == "__main__":
    uvloop.install()
    coordinator_service = CoordinatorService()
    asyncio.run(coordinator_service.main())
