import asyncio
from websockets import serve

import uvloop
from universalis.common.logging import logging
from universalis.common.serialization import msgpack_deserialization, msgpack_serialization

SERVER_PORT = 8888


async def receive_data_discovery(websocket, _):
    async for msg in websocket:
        deserialized_data = msgpack_deserialization(msg)
        if '__COM_TYPE__' not in deserialized_data:
            logging.error(f"Deserialized data do not contain a message type")
        else:
            message_type: str = deserialized_data['__COM_TYPE__']
            message: dict = deserialized_data['__MSG__']
            if message_type == 'DISCOVER':
                reply = msgpack_serialization(operator_partition_locations)
                logging.info(f"DISCOVER reply: {reply}")
                await websocket.send(reply)
            elif message_type == 'REGISTER_OPERATOR_DISCOVERY':
                # RECEIVE MESSAGE FROM COORDINATOR TO REGISTER AN OPERATORS LOCATION i.e. (in which worker it resides)
                operator_name, partition_number, address, port = message
                logging.info(f"REGISTER_OPERATOR_DISCOVERY: {operator_partition_locations}")
                if operator_name in operator_partition_locations:
                    operator_partition_locations[operator_name].update({str(partition_number): (address, port)})
                else:
                    operator_partition_locations[operator_name] = {str(partition_number): (address, port)}
            else:
                logging.error(f"INGRESS SERVER: Non supported message type: {message_type}")


async def main():
    async with serve(receive_data_discovery, "0.0.0.0", SERVER_PORT):
        logging.info(f"Discovery Server listening at 0.0.0.0:{SERVER_PORT}")
        await asyncio.Future()  # run forever

if __name__ == "__main__":
    operator_partition_locations: dict[dict[str, tuple[str, int]]] = {}
    uvloop.install()
    asyncio.run(main())
