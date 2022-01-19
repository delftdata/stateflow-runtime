import asyncio

import aiojobs
import aiozmq
import uvloop
import zmq

from universalis.common.networking import NetworkingManager
from universalis.common.logging import logging
from universalis.common.serialization import Serializer


SERVER_PORT = 8888
INTERNAL_WATERMARK_SECONDS = 0.005  # 5ms
queue_lock = asyncio.Lock()
network_buffer = []


async def send_queues(executor_ipc_client, networking):
    global network_buffer
    while True:
        await asyncio.sleep(INTERNAL_WATERMARK_SECONDS)
        if len(network_buffer) > 0:
            async with queue_lock:
                process_queue_msg = {"__COM_TYPE__": "BATCH", "__MSG__": network_buffer}
                executor_ipc_client.write((networking.encode_message(process_queue_msg, Serializer.MSGPACK), ))
                network_buffer = []


async def main():
    router = await aiozmq.create_zmq_stream(zmq.ROUTER, bind=f"tcp://0.0.0.0:{SERVER_PORT}")
    scheduler = await aiojobs.create_scheduler(limit=None)
    executor_ipc_client = await aiozmq.create_zmq_stream(zmq.DEALER, connect="ipc://executor")
    networking = NetworkingManager()
    await scheduler.spawn(send_queues(executor_ipc_client, networking))
    logging.info(f"Worker TCP Server listening at 0.0.0.0:{SERVER_PORT}")
    while True:
        resp_adr, data = await router.read()
        deserialized_data: dict = networking.decode_message(data)
        if '__COM_TYPE__' not in deserialized_data:
            logging.error(f"Deserialized data do not contain a message type")
        else:
            message_type: str = deserialized_data['__COM_TYPE__']
            message = deserialized_data['__MSG__']
            if message_type == 'RUN_FUN':  # FIRE AND FORGET FUNCTION EXECUTION
                # Add message to queue
                async with queue_lock:
                    network_buffer.append(unpack_run_payload(message))
            elif message_type == 'RUN_FUN_RQ_RS':  # REQUEST RESPONSE
                logging.warning(f"(W) RUN_FUN_RQ_RS")
                # BYPASS QUEUE and run immediately
                process_rq_rs_msg = {"__COM_TYPE__": "RQ_RS",
                                     "__MSG__": unpack_run_payload(message)}
                executor_ipc_client.write((networking.encode_message(process_rq_rs_msg, Serializer.MSGPACK), ))
                response = await executor_ipc_client.read()
                router.write((resp_adr, response[0]))
            elif message_type == 'RECEIVE_EXE_PLN':  # RECEIVE EXECUTION PLAN OF A DATAFLOW GRAPH
                executor_ipc_client.write((data, ))
            else:
                logging.error(f"Worker Service: Non supported command message type: {message_type}")


def unpack_run_payload(message: dict):
    return message['__TIMESTAMP__'], message['__OP_NAME__'], message['__PARTITION__'], message['__FUN_NAME__'], \
           message['__KEY__'], message['__PARAMS__']


if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
