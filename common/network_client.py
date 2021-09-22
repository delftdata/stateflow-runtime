import asyncio
import cloudpickle

from common.logging import logging
from common.serialization import msgpack_serialization


class NetworkTCPClient:

    def __init__(self,
                 networking_process_address: str = '0.0.0.0',
                 networking_process_port: int = 8888):
        self.networking_process_address = networking_process_address
        self.networking_process_port = networking_process_port

    async def async_transmit_tcp_no_response(self, message: object, com_type: str = "NO_RESP") -> None:
        _, writer = await asyncio.open_connection(self.networking_process_address, self.networking_process_port)
        if com_type == "NO_RESP":
            writer.write(msgpack_serialization({"__COM_TYPE__": "NO_RESP", "__MSG__": message}))
        elif com_type == "REMOTE_FUN_CALL":
            writer.write(msgpack_serialization({"__COM_TYPE__": "REMOTE_FUN_CALL", "__MSG__": message}))
        elif com_type == "SCHEDULE_OPERATOR":
            writer.write(cloudpickle.dumps({"__COM_TYPE__": "SCHEDULE_OPERATOR", "__MSG__": message}))
        elif com_type == "REGISTER_OPERATOR_INGRESS":
            writer.write(cloudpickle.dumps({"__COM_TYPE__": "REGISTER_OPERATOR_INGRESS", "__MSG__": message}))
        elif com_type == 'RUN_FUN':
            writer.write(cloudpickle.dumps({"__COM_TYPE__": "RUN_FUN", "__MSG__": message}))
        else:
            logging.error(f"Invalid communication type: {com_type}")
        await writer.drain()
        writer.close()
