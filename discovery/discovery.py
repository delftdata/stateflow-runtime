import asyncio
import uvloop

from universalis.common.logging import logging
from universalis.common.networking import decode_messages, encode_message
from universalis.common.serialization import Serializer

SERVER_PORT = 8888


class DiscoveryProtocol(asyncio.Protocol):

    def __init__(self):
        self.transport: asyncio.Transport = asyncio.Transport()

    def connection_made(self, transport: asyncio.Transport):
        peername = transport.get_extra_info('peername')
        logging.info(f"Connection from {peername}")
        self.transport = transport

    def data_received(self, data):
        deserialized_data: dict
        for deserialized_data in decode_messages(data):
            if '__COM_TYPE__' not in deserialized_data:
                logging.error(f"Deserialized data do not contain a message type")
            else:
                message_type: str = deserialized_data['__COM_TYPE__']
                message: dict = deserialized_data['__MSG__']
                if message_type == 'DISCOVER':
                    reply = encode_message(operator_partition_locations, Serializer.MSGPACK)
                    self.transport.write(reply)
                elif message_type == 'REGISTER_OPERATOR_DISCOVERY':
                    # RECEIVE MESSAGE FROM COORDINATOR TO REGISTER AN OPERATORS LOCATION (in which worker it resides)
                    operator_name, partition_number, address, port = message
                    logging.info(f"REGISTER_OPERATOR_DISCOVERY: {operator_partition_locations}")
                    if operator_name in operator_partition_locations:
                        operator_partition_locations[operator_name].update({str(partition_number): (address, port)})
                    else:
                        operator_partition_locations[operator_name] = {str(partition_number): (address, port)}
                else:
                    logging.error(f"INGRESS SERVER: Non supported message type: {message_type}")


async def main():
    loop = asyncio.get_running_loop()
    server = await loop.create_server(lambda: DiscoveryProtocol(), '0.0.0.0', SERVER_PORT)
    logging.info(f"Discovery Server listening at 0.0.0.0:{SERVER_PORT}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    operator_partition_locations: dict[dict[str, tuple[str, int]]] = {}
    uvloop.install()
    asyncio.run(main())
