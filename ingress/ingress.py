import asyncio
import cloudpickle
import uvloop
from socket import socket, AF_INET, SOCK_STREAM

from common.logging import logging
from common.serialization import msgpack_deserialization
from common.stateflow_worker import StateflowWorker


SERVER_PORT = 8888
registered_routable_operators: dict[str, StateflowWorker] = {}


class IngressProtocol(asyncio.Protocol):

    transport: asyncio.Transport
    dns: dict[str, tuple[str, int]]

    def __init__(self):
        logging.debug("CREATING NEW PROTOCOL")

    def connection_made(self, transport: asyncio.Transport):
        logging.debug(f"Connection from {transport.get_extra_info('peername')}")
        self.transport = transport

    def data_received(self, data: bytes):
        logging.debug(f"data: {data}")
        deserialized_data: dict = msgpack_deserialization(data)
        if '__COM_TYPE__' not in deserialized_data:
            logging.error(f"Deserialized data do not contain a message type")
        else:
            message_type: str = deserialized_data['__COM_TYPE__']
            message: dict = deserialized_data['__MSG__']
            if message_type == 'REGISTER_OPERATOR_INGRESS':
                operator_name, address, port = message
                logging.debug(f"REGISTER_OPERATOR_INGRESS: {registered_routable_operators}")
                registered_routable_operators[operator_name] = StateflowWorker(address, port)
            elif message_type == 'REMOTE_FUN_CALL':
                logging.debug(f"REMOTE_FUN_CALL: {message}")
                operator_name = message['__OP_NAME__']
                worker: StateflowWorker = registered_routable_operators[operator_name]
                try:
                    with socket(AF_INET, SOCK_STREAM) as s:
                        s.connect((worker.host, worker.port))
                        s.setblocking(False)
                        s.sendall(cloudpickle.dumps({"__COM_TYPE__": "INVOKE_LOCAL", "__MSG__": message}))
                except ConnectionRefusedError:
                    logging.error(f"Failed to connect to server {worker.host}:{worker.port}")
            else:
                logging.error(f"TCP SERVER: Non supported message type: {message_type}")
        self.transport.close()


async def main():
    logging.info(f"Ingress Network Protocol Server listening at 0.0.0.0:{SERVER_PORT}")

    loop = asyncio.get_running_loop()

    server = await loop.create_server(lambda: IngressProtocol(), '0.0.0.0', SERVER_PORT)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
