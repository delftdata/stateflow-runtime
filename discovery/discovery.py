import asyncio
from asyncio import StreamWriter

import uvloop
from universalis.common.logging import logging
from universalis.common.serialization import msgpack_deserialization, msgpack_serialization
from universalis.common.stateflow_worker import StateflowWorker

SERVER_PORT = 8888


async def receive_data_discovery(reader, writer: StreamWriter):
    data: bytes = await reader.read()
    deserialized_data = msgpack_deserialization(data)
    if '__COM_TYPE__' not in deserialized_data:
        logging.error(f"Deserialized data do not contain a message type")
    else:
        message_type: str = deserialized_data['__COM_TYPE__']
        message: dict = deserialized_data['__MSG__']
        if message_type == 'DISCOVER':
            writer.write(msgpack_serialization(message))
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        elif message_type == 'REGISTER_OPERATOR_DISCOVERY':
            # RECEIVE MESSAGE FROM COORDINATOR TO REGISTER AN OPERATORS LOCATION i.e. (in which worker it resides)
            operator_name, partition_number, address, port = message
            logging.debug(f"REGISTER_OPERATOR_DISCOVERY: {operator_partition_locations}")
            if operator_name in operator_partition_locations:
                operator_partition_locations[operator_name].update({partition_number: StateflowWorker(address, port)})
            else:
                operator_partition_locations[operator_name] = {partition_number: StateflowWorker(address, port)}
        else:
            logging.error(f"INGRESS SERVER: Non supported message type: {message_type}")


async def main():
    server = await asyncio.start_server(receive_data_discovery, '0.0.0.0', SERVER_PORT)
    logging.info(f"Ingress Server listening at 0.0.0.0:{SERVER_PORT}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    operator_partition_locations: dict[dict[int, StateflowWorker]] = {}
    uvloop.install()
    asyncio.run(main())
