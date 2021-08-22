import asyncio
import os
import uvloop
import logging

from utils import msgpack_deserialization, msgpack_serialization, benchmark_peers, transmit_tcp_no_response

logging.basicConfig(level=logging.INFO)

OWN_ADDRESS_NAME = os.getenv('OWN_ADDRESS_NAME')
OWN_PORT = os.getenv('OWN_PORT')
SERVER_PORT = 8888
OPERATOR_SERVER_PORT = 8889


class WorkerNetworkProtocol(asyncio.Protocol):

    transport: asyncio.Transport
    peers: list[tuple[str, int]]
    dns: dict[str, tuple[str, int]]

    def __init__(self):
        logging.debug("CREATING NEW PROTOCOL")
        self.peers = eval(os.environ.get("PEERS", "[]"))
        logging.debug(f"PEERS: {self.peers}")
        self.own_address = f"{OWN_ADDRESS_NAME}:{OWN_PORT}"
        self.dns = {'user': ('worker-0', 8888), 'stock': ('worker-1', 8888), 'order': ('worker-2', 8888)}

    def connection_made(self, transport: asyncio.Transport):
        logging.debug(f"Connection from {transport.get_extra_info('peername')}")
        logging.debug(f"Peers: {self.peers}")
        self.transport = transport

    def data_received(self, data: bytes):
        deserialized_data: dict = msgpack_deserialization(data)
        if '__COM_TYPE__' not in deserialized_data:
            logging.error(f"Deserialized data do not contain a message type")
        else:
            message_type: str = deserialized_data['__COM_TYPE__']
            message = deserialized_data['__MSG__']
            if message_type == 'NO_RESP':
                logging.debug(f"NO_RESP: {message}")
            elif message_type == 'GET_PEERS':
                peers = [peer for peer in message if f"{peer[0]}:{peer[1]}" != f"0.0.0.0:{SERVER_PORT}"]
                logging.debug(f"Peers received: {peers}")
                os.environ["PEERS"] = str(peers)
                asyncio.ensure_future(benchmark_peers(peers))
            elif message_type == 'SC1':
                transmit_tcp_no_response('0.0.0.0', OPERATOR_SERVER_PORT, message, com_type='SC1')
            elif message_type == 'REQ_RESP':
                logging.debug(f"REQ_RESP: {message}")
                response = msgpack_serialization(message)
                logging.debug(f"SEND RESPONSE: {message}")
                self.transport.write(response)
            elif message_type == 'REMOTE_FUN_CALL':
                logging.debug(f"REMOTE_FUN_CALL: {message}")
                operator_name = message['__OP_NAME__']
                operator_address, operator_port = self.dns[operator_name]
                transmit_tcp_no_response(operator_address, operator_port, message, com_type='INVOKE_LOCAL')
            elif message_type == 'INVOKE_LOCAL':
                logging.debug(f"INVOKE_LOCAL: {message}")
                transmit_tcp_no_response('0.0.0.0', OPERATOR_SERVER_PORT, message, com_type='RUN_FUN')
            else:
                logging.error(f"TCP SERVER: Non supported message type: {message_type}")
        self.transport.close()


async def main():
    logging.info(f"Worker Network Protocol Server listening at 0.0.0.0:{SERVER_PORT}")

    loop = asyncio.get_running_loop()

    server = await loop.create_server(lambda: WorkerNetworkProtocol(), '0.0.0.0', SERVER_PORT)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
