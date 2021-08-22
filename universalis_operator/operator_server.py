import asyncio
import msgpack
import os
import uvloop
import logging


from opeartor import Operator
# from universalis_operator.utils import start_scenario


logging.basicConfig(level=logging.INFO)

OWN_ADDRESS_NAME = os.getenv('OWN_ADDRESS_NAME')
OWN_PORT = os.getenv('OWN_PORT')
SERVER_PORT = 8889
registered_operators: dict[str, Operator] = {}


def msgpack_serialization(serializable_object: object) -> bytes:
    return msgpack.packb(serializable_object)


def msgpack_deserialization(serialized_object: bytes) -> dict:
    return msgpack.unpackb(serialized_object)


class OperatorServerIPCProtocol(asyncio.Protocol):

    transport: asyncio.Transport

    def __init__(self):
        logging.debug("Creating new OperatorServerIPCProtocol")

    def connection_made(self, transport: asyncio.Transport):
        logging.debug(f"Connection from {transport.get_extra_info('peername')}")
        self.transport = transport

    def data_received(self, data: bytes):
        deserialized_data: dict = msgpack_deserialization(data)
        if '__COM_TYPE__' not in deserialized_data:
            logging.error(f"Deserialized data do not contain a message type")
        else:
            message_type: str = deserialized_data['__COM_TYPE__']
            message = deserialized_data['__MSG__']
            if message_type == 'RUN_FUN':
                operator_name = message['__OP_NAME__']
                function_name = message['__FUN_NAME__']
                function_params = message['__PARAMS__']
                operator = registered_operators[operator_name]
                function_to_invoke = operator.functions[function_name]
                if operator_name == 'user':
                    params = (function_params['user_key'], function_params['credit'])
                    logging.info(
                        f'Running function {function_name} of operator {operator_name} with parameters {params}')
                    function_to_invoke(*params)
                elif operator_name == 'stock':
                    params = (function_params['item_key'], function_params['quantity'])
                    logging.info(
                        f'Running function {function_name} of operator {operator_name} with parameters {params}')
                    function_to_invoke(*params)
            elif message_type == 'SC1':
                pass
                # asyncio.ensure_future(start_scenario(OWN_ADDRESS_NAME, registered_operators))
            else:
                logging.error(f"Operator Server: Non supported message type: {message_type}")
        self.transport.close()


async def main():
    logging.info(f"Operator Server listening at 0.0.0.0:{SERVER_PORT}")

    loop = asyncio.get_running_loop()

    server = await loop.create_server(lambda: OperatorServerIPCProtocol(), '0.0.0.0', SERVER_PORT)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
