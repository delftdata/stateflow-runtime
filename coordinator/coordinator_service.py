import asyncio
import uvloop
import cloudpickle

from common.networking import async_transmit_tcp_no_response
from common.logging import logging
from common.stateflow_worker import StateflowWorker
from common.operator import Operator

from coordinator import Coordinator

SERVER_PORT = 8888


async def receive_data_coordinator(reader, writer):
    data: bytes = await reader.read()
    deserialized_data: dict = cloudpickle.loads(data)
    if '__COM_TYPE__' not in deserialized_data:
        logging.error(f"Deserialized data do not contain a message type")
    else:
        message_type: str = deserialized_data['__COM_TYPE__']
        message = deserialized_data['__MSG__']
        if message_type == 'SCHEDULE_OPERATOR':
            # Received scheduling requests from  coordinator to transmit to workers
            logging.info(f"Scheduling: {message}")
            operator: Operator
            target_worker: StateflowWorker
            operator_name, partition, operator, dns, target_worker = message
            schedule_operator_message = (operator, partition, dns)
            await async_transmit_tcp_no_response(target_worker.host,
                                                 target_worker.port,
                                                 schedule_operator_message,
                                                 com_type='RECEIVE_EXE_PLN')
        elif message_type == 'SEND_EXECUTION_GRAPH':
            # Received execution graph from a universalis client
            coordinator = Coordinator()
            serving_ingress = await coordinator.submit_stateflow_graph(message)
            logging.info(f"SERVING_INGRESS: {serving_ingress}")
            writer.write(cloudpickle.dumps(serving_ingress))
            await writer.drain()
            logging.info(f"Closing writer")
            writer.close()
            await writer.wait_closed()
        elif message_type == 'REGISTER_OPERATOR_INGRESS':
            # Register the operator addresses to an ingress
            logging.info(f"REGISTER_OPERATOR_INGRESS: {message}")
            operator_name, partition, operator_host, operator_port, ingress_host, ingress_port = message
            await async_transmit_tcp_no_response(ingress_host,
                                                 ingress_port,
                                                 (operator_name, partition, operator_host, operator_port),
                                                 com_type='REGISTER_OPERATOR_INGRESS')
        else:
            logging.error(f"COORDINATOR SERVER: Non supported message type: {message_type}")


async def main():
    server = await asyncio.start_server(receive_data_coordinator, '0.0.0.0', SERVER_PORT)

    logging.info(f"Coordinator Server listening at 0.0.0.0:{SERVER_PORT}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
