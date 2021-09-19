import asyncio
import os
import uvloop
import logging
import cloudpickle

from common.networking import transmit_tcp_no_response

from common.stateflow_worker import StateflowWorker
from common.opeartor import Operator

# TODO make the coordinator as a standalone tcp service
from coordinator.coordinator import Coordinator

logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s',
                    level=logging.INFO,
                    datefmt='%H:%M:%S')

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
        self.coordinator = Coordinator()

    def connection_made(self, transport: asyncio.Transport):
        logging.debug(f"Connection from {transport.get_extra_info('peername')}")
        logging.debug(f"Peers: {self.peers}")
        self.transport = transport

    def data_received(self, data: bytes):
        logging.debug(f"data: {data}")
        deserialized_data: dict = cloudpickle.loads(data)
        if '__COM_TYPE__' not in deserialized_data:
            logging.error(f"Deserialized data do not contain a message type")
        else:
            message_type: str = deserialized_data['__COM_TYPE__']
            message = deserialized_data['__MSG__']
            if message_type == 'NO_RESP':
                logging.debug(f"NO_RESP: {message}")
            elif message_type == 'SC1':
                transmit_tcp_no_response('0.0.0.0', OPERATOR_SERVER_PORT, message, com_type='SC1')
            elif message_type == 'REQ_RESP':
                logging.debug(f"REQ_RESP: {message}")
                response = cloudpickle.loads(message)
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
            elif message_type == 'SCHEDULE_OPERATOR':
                # Received scheduling requests from  coordinator to transmit to workers
                logging.info(f"Scheduling: {message}")
                operator: Operator
                target_worker: StateflowWorker
                operator_name, operator, dns, target_worker = message
                schedule_operator_message = (operator, dns)
                transmit_tcp_no_response(target_worker.host,
                                         target_worker.port,
                                         schedule_operator_message,
                                         com_type='RECEIVE_EXE_PLN')
            elif message_type == 'SEND_EXECUTION_GRAPH':
                # Received execution graph from a universalis client
                self.transport.write(cloudpickle.dumps(self.coordinator.submit_stateflow_graph(message)))
            elif message_type == 'RECEIVE_EXE_PLN':
                # Receive operator from coordinator
                logging.info(f"RECEIVE_EXE_PLN: {message}")
                transmit_tcp_no_response(OWN_ADDRESS_NAME, OPERATOR_SERVER_PORT, message, com_type='REGISTER_OPERATOR')
            elif message_type == 'RUN_FUN':
                # Receive data message to be routed to an operator
                logging.info(f"RUN_FUN: {message}")
                transmit_tcp_no_response('0.0.0.0', OPERATOR_SERVER_PORT, message, com_type='RUN_FUN')
            elif message_type == 'REGISTER_OPERATOR_INGRESS':
                logging.info(f"REGISTER_OPERATOR_INGRESS: {message}")
                operator_name, operator_host, operator_port, ingress_host, ingress_port = message
                transmit_tcp_no_response(ingress_host, ingress_port, (operator_name, operator_host, operator_port),
                                         com_type='REGISTER_OPERATOR_INGRESS')
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
