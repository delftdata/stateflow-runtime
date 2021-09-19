import asyncio
import uvloop
import cloudpickle

from common.stateflow_worker import StateflowWorker
from common.opeartor import Operator
from common.logging import logging

SERVER_PORT = 8889
registered_operators: dict[str, Operator] = {}


class OperatorServerIPCProtocol(asyncio.Protocol):

    transport: asyncio.Transport

    def __init__(self):
        logging.debug("Creating new OperatorServerIPCProtocol")

    def connection_made(self, transport: asyncio.Transport):
        logging.debug(f"Connection from {transport.get_extra_info('peername')}")
        self.transport = transport

    def data_received(self, data: bytes):
        deserialized_data: dict = cloudpickle.loads(data)
        if '__COM_TYPE__' not in deserialized_data:
            logging.error(f"Deserialized data do not contain a message type")
        else:
            message_type: str = deserialized_data['__COM_TYPE__']
            message = deserialized_data['__MSG__']
            if message_type == 'RUN_FUN':
                operator_name = message['__OP_NAME__']
                function_name = message['__FUN_NAME__']
                function_params = message['__PARAMS__']
                timestamp = message['__TIMESTAMP__']
                logging.info(f'OPERATOR SERVER: Running function {function_name} '
                             f'of operator: {operator_name} with params: {function_params} with timestamp: {timestamp}')
                logging.debug(f'OPERATOR SERVER: Running function {function_name} '
                              f'of operator: {operator_name} with params: {function_params}')
                registered_operators[operator_name].functions[function_name](*function_params)  # Run function
            elif message_type == 'REGISTER_OPERATOR':
                operator: Operator
                dns: dict[str, StateflowWorker]
                operator, dns = message
                operator.set_dns(dns)
                registered_operators[operator.name] = operator
                logging.info(f'Registered operators: {registered_operators}')
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
