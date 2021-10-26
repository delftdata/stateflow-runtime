import asyncio
import uvloop
import cloudpickle

from universalis.common.logging import logging

from coordinator import Coordinator

SERVER_PORT = 8888


async def receive_data_coordinator(reader, _):
    data: bytes = await reader.read()
    deserialized_data: dict = cloudpickle.loads(data)
    if '__COM_TYPE__' not in deserialized_data:
        logging.error(f"Deserialized data do not contain a message type")
    else:
        message_type: str = deserialized_data['__COM_TYPE__']
        message = deserialized_data['__MSG__']
        if message_type == 'SEND_EXECUTION_GRAPH':
            # Received execution graph from a universalis client
            coordinator = Coordinator()
            await coordinator.submit_stateflow_graph(message)
        else:
            logging.error(f"COORDINATOR SERVER: Non supported message type: {message_type}")


async def main():
    server = await asyncio.start_server(receive_data_coordinator, '0.0.0.0', SERVER_PORT)

    logging.info(f"Coordinator Server listening at 0.0.0.0:{SERVER_PORT}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
