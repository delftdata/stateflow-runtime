import asyncio
import uuid
from asyncio import StreamWriter

import cloudpickle
import uvloop

from universalis.common.logging import logging
from universalis.common.serialization import msgpack_deserialization
from universalis.common.stateflow_worker import StateflowWorker

SERVER_PORT = 8888


class StrKeyNotUUID(Exception):
    pass


class NonSupportedKeyType(Exception):
    pass


def make_key_hashable(key):
    if isinstance(key, str):
        try:
            key = uuid.UUID(key)
        except ValueError:
            raise StrKeyNotUUID()
    elif not isinstance(key, int):
        raise NonSupportedKeyType()
    return key


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
            operator_name, partition_number, address, port = message
            logging.debug(f"REGISTER_OPERATOR_INGRESS: {registered_routable_operators}")
            if operator_name in registered_routable_operators:
                registered_routable_operators[operator_name].update({partition_number: StateflowWorker(address, port)})
            else:
                registered_routable_operators[operator_name] = {partition_number: StateflowWorker(address, port)}
        elif message_type == 'REMOTE_FUN_CALL':
            # RECEIVE MESSAGE FROM A CLIENT TO PASS INTO A STATEFLOW GRAPH'S OPERATOR FUNCTION
            operator_name = message['__OP_NAME__']
            key = message['__KEY__']
            try:
                partition: int = int(make_key_hashable(key)) % len(registered_routable_operators[operator_name].keys())
                worker: StateflowWorker = registered_routable_operators[operator_name][partition]
                # if (worker.host, worker.port) not in open_connections:
                logging.info(f"Opening connection to: {worker.host}:{worker.port}")
                _, worker_writer = await asyncio.open_connection(worker.host, worker.port)
                open_connections[(worker.host, worker.port)] = worker_writer
                message.update({'__PARTITION__': partition})
                logging.debug(f'Sending packet: {message} to {worker.host}:{worker.port}')
                open_connections[(worker.host, worker.port)].write(cloudpickle.dumps({"__COM_TYPE__": "RUN_FUN",
                                                                                      "__MSG__": message}))
                await open_connections[(worker.host, worker.port)].drain()
                open_connections[(worker.host, worker.port)].close()
                await open_connections[(worker.host, worker.port)].wait_closed()
            except StrKeyNotUUID:
                logging.error(f"String key: {key} is not a UUID")
            except NonSupportedKeyType:
                logging.error(f"Supported keys are integers and UUIDS not {type(key)}")
        else:
            logging.error(f"INGRESS SERVER: Non supported message type: {message_type}")


async def main():
    server = await asyncio.start_server(receive_data_ingress, '0.0.0.0', SERVER_PORT)
    logging.info(f"Ingress Server listening at 0.0.0.0:{SERVER_PORT}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    registered_routable_operators: dict[dict[int, StateflowWorker]] = {}
    open_connections: dict[tuple, StreamWriter] = {}
    uvloop.install()
    asyncio.run(main())
