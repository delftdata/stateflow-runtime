import asyncio
import os

from websockets import serve
import uvloop

from universalis.common.logging import logging
from universalis.common.networking import async_transmit_websocket_no_response_new_connection, \
    async_transmit_websocket_request_response_new_connection
from universalis.common.serialization import msgpack_deserialization
from universalis.common.stateflow_worker import StateflowWorker
from universalis.common.stateful_function import make_key_hashable, StrKeyNotUUID, NonSupportedKeyType

SERVER_PORT = 8888
DISCOVERY_HOST = os.environ['DISCOVERY_HOST']
DISCOVERY_PORT = int(os.environ['DISCOVERY_PORT'])


async def get_registered_operators() -> dict[dict[int, StateflowWorker]]:
    return await async_transmit_websocket_request_response_new_connection(DISCOVERY_HOST,
                                                                          DISCOVERY_PORT,
                                                                          "",
                                                                          com_type="DISCOVER")


async def receive_data_ingress(websocket, _):
    global registered_operators
    async for msg in websocket:
        deserialized_message = msgpack_deserialization(msg)
        if '__COM_TYPE__' not in deserialized_message:
            logging.error(f"Deserialized data do not contain a message type")
        else:
            message_type: str = deserialized_message['__COM_TYPE__']
            message: dict = deserialized_message['__MSG__']
            if message_type == 'REMOTE_FUN_CALL':
                # RECEIVE MESSAGE FROM A CLIENT TO PASS INTO A STATEFLOW GRAPH'S OPERATOR FUNCTION
                operator_name = message['__OP_NAME__']
                key = message['__KEY__']
                try:
                    try:
                        partition: str = str(
                            int(make_key_hashable(key)) % len(registered_operators[operator_name].keys()))
                        worker: tuple[str, int] = registered_operators[operator_name][partition]
                    except KeyError:
                        registered_operators = await get_registered_operators()
                        logging.info(registered_operators)
                        partition: str = str(
                            int(make_key_hashable(key)) % len(registered_operators[operator_name].keys()))
                        worker: tuple[str, int] = registered_operators[operator_name][partition]
                    message.update({'__PARTITION__': int(partition)})
                    await async_transmit_websocket_no_response_new_connection(worker[0],
                                                                              worker[1],
                                                                              message,
                                                                              com_type="RUN_FUN")
                except StrKeyNotUUID:
                    logging.error(f"String key: {key} is not a UUID")
                except NonSupportedKeyType:
                    logging.error(f"Supported keys are integers and UUIDS not {type(key)}")
            else:
                logging.error(f"INGRESS SERVER: Non supported message type: {message_type}")


async def main():
    async with serve(receive_data_ingress, "0.0.0.0", SERVER_PORT):
        logging.info(f"Ingress Server listening at 0.0.0.0:{SERVER_PORT}")
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    registered_operators: dict[dict[str, tuple[str, int]]] = {}
    open_connections = {}
    uvloop.install()
    asyncio.run(main())
