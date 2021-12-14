import asyncio
import uvloop

from universalis.common.logging import logging
from universalis.common.networking import decode_messages, encode_message
from universalis.common.serialization import Serializer

from coordinator import Coordinator

SERVER_PORT = 8888


class CoordinatorServerProtocol(asyncio.Protocol):

    def __init__(self):
        self.transport: asyncio.Transport = asyncio.Transport()

    def connection_made(self, transport: asyncio.Transport):
        peername = transport.get_extra_info('peername')
        logging.info(f"Connection from {peername}")
        self.transport = transport

    def data_received(self, data):
        global operator_partition_locations
        deserialized_data: dict
        for deserialized_data in decode_messages(data):
            if '__COM_TYPE__' not in deserialized_data:
                logging.error(f"Deserialized data do not contain a message type")
            else:
                message_type: str = deserialized_data['__COM_TYPE__']
                message = deserialized_data['__MSG__']
                if message_type == 'SEND_EXECUTION_GRAPH':
                    # Received execution graph from a universalis client
                    coordinator = Coordinator()
                    operator_partition_locations = coordinator.submit_stateflow_graph(message)
                elif message_type == 'DISCOVER':
                    reply = encode_message(operator_partition_locations, Serializer.MSGPACK)
                    self.transport.write(reply)
                else:
                    logging.error(f"COORDINATOR SERVER: Non supported message type: {message_type}")


async def main():
    loop = asyncio.get_running_loop()
    server = await loop.create_server(lambda: CoordinatorServerProtocol(), '0.0.0.0', SERVER_PORT)
    logging.info(f"Coordinator Server listening at 0.0.0.0:{SERVER_PORT}")

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    operator_partition_locations: dict[dict[str, tuple[str, int]]] = {}
    uvloop.install()
    asyncio.run(main())
