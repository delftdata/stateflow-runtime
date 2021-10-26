import uvloop
import cloudpickle

from asyncio import PriorityQueue, sleep, get_running_loop, start_server, run

from universalis.common.logging import logging
from universalis.common.operator import Operator
from universalis.common.stateflow_worker import StateflowWorker

SERVER_PORT = 8888
INTERNAL_WATERMARK_SECONDS = 0.005  # 5ms


async def receive_data_tcp(reader, _):
    data: bytes = await reader.read()
    deserialized_data: dict = cloudpickle.loads(data)
    logging.info('RECEIVED MESSAGE')
    if '__COM_TYPE__' not in deserialized_data:
        logging.error(f"Deserialized data do not contain a message type")
    else:
        message_type: str = deserialized_data['__COM_TYPE__']
        message = deserialized_data['__MSG__']
        if message_type == 'RUN_FUN':
            operator_name: str = message['__OP_NAME__']
            partition: int = message['__PARTITION__']
            function_name: str = message['__FUN_NAME__']
            function_params = message['__PARAMS__']
            timestamp: int = message['__TIMESTAMP__']
            logging.debug(f"Running {operator_name}|{partition}:{function_name} "
                          f"with params: {function_params} at time: {timestamp}")
            queue_entry = timestamp, function_name, function_params
            await operator_queues[operator_name][partition].put(queue_entry)
        elif message_type == 'REMOTE_RUN_FUN':
            pass
        elif message_type == 'RECEIVE_EXE_PLN':
            # Receive operator from coordinator
            operator: Operator
            dns: dict[str, StateflowWorker]
            operator, partition, dns = message
            operator.set_dns(dns)
            if operator.name in registered_operators:
                registered_operators[operator.name].update({partition: operator})
                operator_queues[operator.name].update({partition: PriorityQueue()})
            else:
                registered_operators[operator.name] = {partition: operator}
                operator_queues[operator.name] = {partition: PriorityQueue()}
            logging.info(f'Registered operators: {registered_operators}')
        else:
            logging.error(f"TCP SERVER: Non supported message type: {message_type}")


async def process_queue():
    while True:
        for operator_name, partitioned_queues in operator_queues.items():
            for partition, q in partitioned_queues.items():
                while not q.empty():
                    queue_value = await q.get()
                    timestamp, function_name, params = queue_value
                    await registered_operators[operator_name][partition].run_function(function_name, *params)
        await sleep(INTERNAL_WATERMARK_SECONDS)


async def main():
    server = await start_server(receive_data_tcp, '0.0.0.0', SERVER_PORT)
    logging.info(f"Worker Service listening at 0.0.0.0:{SERVER_PORT}")

    loop = get_running_loop()
    loop.create_task(process_queue())
    logging.info('Queue ingestion timer registered')

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    registered_operators: dict[str, dict[int, Operator]] = {}
    operator_queues: dict[str, dict[int, PriorityQueue]] = {}
    uvloop.install()
    run(main())
