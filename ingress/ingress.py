import asyncio
import uvloop

from universalis.common.logging import logging
from universalis.common.networking import decode_messages

from commands import remote_fun_call

SERVER_PORT = 8888


class IngressServerProtocol(asyncio.Protocol):

    def __init__(self):
        self.transport: asyncio.Transport = asyncio.Transport()

    def connection_made(self, transport: asyncio.Transport):
        peername = transport.get_extra_info('peername')
        logging.info(f"Connection from {peername}")
        self.transport = transport

    def data_received(self, data):
        deserialized_data: dict
        for deserialized_data in decode_messages(data):
            if '__COM_TYPE__' not in deserialized_data:
                logging.error(f"Deserialized data do not contain a message type")
            else:
                message_type: str = deserialized_data['__COM_TYPE__']
                message: dict = deserialized_data['__MSG__']
                if message_type == 'REMOTE_FUN_CALL':
                    logging.warning(f"Received message that calls function {message['__FUN_NAME__']}")
                    # RECEIVE MESSAGE FROM A CLIENT TO PASS INTO A STATEFLOW GRAPH'S OPERATOR FUNCTION
                    remote_fun_call(message)
                else:
                    logging.error(f"INGRESS SERVER: Non supported message type: {message_type}")


async def main():
    loop = asyncio.get_running_loop()
    server = await loop.create_server(lambda: IngressServerProtocol(), '0.0.0.0', SERVER_PORT)
    logging.info(f"Ingress Server listening at 0.0.0.0:{SERVER_PORT}")

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
