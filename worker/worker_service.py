import os

import uvloop
import asyncio

from universalis.common.logging import logging
from universalis.common.networking import decode_messages, NetworkingManager, encode_message
from universalis.common.operator import Operator
from universalis.common.serialization import Serializer, msgpack_serialization

from worker.commands import run_fun, receive_exe_plan
from worker.operator_state.redis_state import RedisOperatorState

SERVER_PORT = 8888
DISCOVERY_HOST = os.environ['DISCOVERY_HOST']
DISCOVERY_PORT = int(os.environ['DISCOVERY_PORT'])
INTERNAL_WATERMARK_SECONDS = 0.005  # 5ms
MAX_OUTER_MESSAGES_TO_PROC = 10
networking_manager: NetworkingManager = NetworkingManager()
dns = {}


def get_registered_operators():
    global networking_manager
    networking_manager.send_message(DISCOVERY_HOST,
                                    DISCOVERY_PORT,
                                    "",
                                    "",
                                    {"__COM_TYPE__": "DISCOVER", "__MSG__": ""},
                                    Serializer.MSGPACK)
    return networking_manager.receive_message(DISCOVERY_HOST, DISCOVERY_PORT, "", "")


class WorkerServerProtocol(asyncio.Protocol):

    # peer_name: str
    transport: asyncio.Transport

    def connection_made(self, transport: asyncio.Transport):
        # self.peer_name: str = transport.get_extra_info('peername')
        # logging.info(f"Connection from {self.peer_name}")
        self.transport = transport

    def data_received(self, data):
        global registered_operator_connections, registered_operators, operator_queues
        deserialized_data: dict
        for deserialized_data in decode_messages(data):
            logging.info('RECEIVED MESSAGE')
            if '__COM_TYPE__' not in deserialized_data:
                logging.error(f"Deserialized data do not contain a message type")
            else:
                message_type: str = deserialized_data['__COM_TYPE__']
                message = deserialized_data['__MSG__']
                if message_type == 'RUN_FUN':
                    run_fun(message, operator_queues)
                elif message_type == 'RUN_FUN_RQ_RS':
                    logging.warning(f"(W) RUN_FUN_RQ_RS")
                    run_fun(message, operator_queues, response_host_name=self.transport)
                # elif message_type == 'GET_RESPONSE':
                #     logging.warning(f"(W) Received response")
                #     request_id: str = message['__RQ_ID__']
                #     response: str = message['__RSP__']
                #     request_response_db.put_no_wait(request_id, response)
                elif message_type == 'RECEIVE_EXE_PLN':
                    # Receive operator from coordinator
                    receive_exe_plan(message, registered_operators, operator_queues)
                else:
                    logging.error(f"Worker Service: Non supported command message type: {message_type}")


async def process_operator_queues():
    global dns
    while True:
        for operator_name, partitioned_queues in operator_queues.items():
            for partition, q in partitioned_queues.items():
                while not q.empty():
                    # logging.warning(f'(W) -> Processing queue for operator: {operator_name}-{partition}')
                    queue_value = q.get_nowait()
                    timestamp, function_name, key, params, response_host = queue_value
                    logging.warning(f'Running function {function_name} with params {params} at {timestamp}')
                    registered_operators[operator_name][partition].set_function_timestamp(function_name, timestamp)
                    if operator_name not in dns:
                        dns = get_registered_operators()
                        # logging.warning(f'DNS RECEIVED: {dns}')
                    registered_operators[operator_name][partition].set_function_dns(function_name, dns)
                    res = await registered_operators[operator_name][partition].run_function(function_name, *params)
                    if response_host is not None:
                        # response_msg = {"__COM_TYPE__": "GET_RESPONSE",
                        #                 "__MSG__": {
                        #                     '__RQ_ID__': key,
                        #                     '__RSP__': res,
                        #                     '__TIMESTAMP__': timestamp
                        #                 }}
                        logging.warning(f'(W) -> SENDING RESPONSE')
                        response_host.write(encode_message(res, Serializer.MSGPACK))
                        response_host.close()
                        # networking.send_message(response_host[0], SERVER_PORT, response_msg, Serializer.MSGPACK)
        await asyncio.sleep(INTERNAL_WATERMARK_SECONDS)


async def main():
    loop = asyncio.get_running_loop()
    server = await loop.create_server(lambda: WorkerServerProtocol(), '0.0.0.0', SERVER_PORT)
    logging.info(f"Worker Service listening at 0.0.0.0:{SERVER_PORT}")

    loop = asyncio.get_running_loop()
    loop.create_task(process_operator_queues())
    logging.info('Queue ingestion timer registered')

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    registered_operators: dict[str, dict[int, Operator]] = {}
    operator_queues: dict[str, dict[int, asyncio.PriorityQueue]] = {}
    registered_operator_connections: dict[dict[str, tuple[str, int]]] = {}
    uvloop.install()
    asyncio.run(main())
