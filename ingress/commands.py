import os

from universalis.common.logging import logging
from universalis.common.networking import NetworkingManager
from universalis.common.serialization import Serializer
from universalis.common.stateflow_worker import StateflowWorker
from universalis.common.stateful_function import make_key_hashable, StrKeyNotUUID, NonSupportedKeyType


DISCOVERY_HOST = os.environ['DISCOVERY_HOST']
DISCOVERY_PORT = int(os.environ['DISCOVERY_PORT'])
registered_operator_connections: dict[dict[str, tuple[str, int]]] = {}
networking_manager: NetworkingManager = NetworkingManager()


def get_registered_operators():
    global networking_manager
    networking_manager.send_message(DISCOVERY_HOST,
                                    DISCOVERY_PORT,
                                    "",
                                    "",
                                    {"__COM_TYPE__": "DISCOVER", "__MSG__": ""},
                                    Serializer.MSGPACK)
    return networking_manager.receive_message(DISCOVERY_HOST, DISCOVERY_PORT, "", "")


def remote_fun_call(message):
    global registered_operator_connections, networking_manager
    operator_name = message['__OP_NAME__']
    function_name = message['__FUN_NAME__']
    key = message['__KEY__']

    if operator_name not in registered_operator_connections:
        registered_operator_connections = get_registered_operators()
    try:
        try:
            partition: str = str(int(make_key_hashable(key)) %
                                 len(registered_operator_connections[operator_name].keys()))
            worker: tuple[str, int] = registered_operator_connections[operator_name][partition]
        except KeyError:
            registered_operator_connections = get_registered_operators()
            logging.info(registered_operator_connections)
            partition: str = str(
                int(make_key_hashable(key)) % len(registered_operator_connections[operator_name].keys()))
            worker: tuple[str, int] = registered_operator_connections[operator_name][partition]
        worker: StateflowWorker = StateflowWorker(worker[0], worker[1])
        logging.debug(f"Opening connection to: {worker.host}:{worker.port}")
        message.update({'__PARTITION__': int(partition)})
        logging.debug(f'Sending packet: {message} to {worker.host}:{worker.port}')
        networking_manager.send_message(worker.host,
                                        worker.port,
                                        operator_name,
                                        function_name,
                                        {"__COM_TYPE__": "RUN_FUN",
                                         "__MSG__": message},
                                        Serializer.MSGPACK)
    except StrKeyNotUUID:
        logging.error(f"String key: {key} is not a UUID")
    except NonSupportedKeyType:
        logging.error(f"Supported keys are integers and UUIDS not {type(key)}")
