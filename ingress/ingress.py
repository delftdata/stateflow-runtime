import asyncio
import cloudpickle
import uvloop

from common.logging import logging
from common.serialization import msgpack_deserialization
from common.stateflow_worker import StateflowWorker

SERVER_PORT = 8888


async def receive_data_ingress(reader, _):
    data: bytes = await reader.read()
    deserialized_data = msgpack_deserialization(data)
    if '__COM_TYPE__' not in deserialized_data:
        logging.error(f"Deserialized data do not contain a message type")
    else:
        message_type: str = deserialized_data['__COM_TYPE__']
        message: dict = deserialized_data['__MSG__']
        if message_type == 'REGISTER_OPERATOR_INGRESS':
            # RECEIVE MESSAGE FROM COORDINATOR TO REGISTER AN OPERATORS LOCATION i.e. (in which worker it resides)
            operator_name, address, port = message
            logging.debug(f"REGISTER_OPERATOR_INGRESS: {registered_routable_operators}")
            registered_routable_operators[operator_name] = StateflowWorker(address, port)
        elif message_type == 'REMOTE_FUN_CALL':
            # RECEIVE MESSAGE FROM A CLIENT TO PASS INTO A STATEFLOW GRAPH'S OPERATOR FUNCTION
            logging.debug(f"REMOTE_FUN_CALL: {message}")
            operator_name = message['__OP_NAME__']
            worker: StateflowWorker = registered_routable_operators[operator_name]
            _, worker_writer = await asyncio.open_connection(worker.host, worker.port)
            logging.debug(f'Sending packet: {message} to {worker.host}:{worker.port}')
            worker_writer.write(cloudpickle.dumps({"__COM_TYPE__": "RUN_FUN", "__MSG__": message}))
            await worker_writer.drain()
            worker_writer.close()
        else:
            logging.error(f"INGRESS SERVER: Non supported message type: {message_type}")


async def main():
    server = await asyncio.start_server(receive_data_ingress, '0.0.0.0', SERVER_PORT)
    logging.info(f"Ingress Server listening at 0.0.0.0:{SERVER_PORT}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    registered_routable_operators: dict[str, StateflowWorker] = {}
    uvloop.install()
    asyncio.run(main())
