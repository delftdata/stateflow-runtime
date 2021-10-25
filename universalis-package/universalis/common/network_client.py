import asyncio
import cloudpickle

from universalis.common.logging import logging
from universalis.common.serialization import msgpack_serialization


class NetworkTCPClient:

    def __init__(self,
                 networking_process_address: str = '0.0.0.0',
                 networking_process_port: int = 8888):
        self.networking_process_address = networking_process_address
        self.networking_process_port = networking_process_port

    async def async_transmit_tcp_no_response(self, message: object, com_type: str = "NO_RESP") -> None:
        _, writer = await asyncio.open_connection(self.networking_process_address, self.networking_process_port)
        if com_type in ["NO_RESP", "REMOTE_FUN_CALL"]:
            writer.write(msgpack_serialization({"__COM_TYPE__": com_type, "__MSG__": message}))
        elif com_type in ["SCHEDULE_OPERATOR", "REGISTER_OPERATOR_INGRESS", "RUN_FUN"]:
            writer.write(cloudpickle.dumps({"__COM_TYPE__": com_type, "__MSG__": message}))
        else:
            logging.error(f"Invalid communication type: {com_type}")
        await writer.drain()
        writer.close()
        await writer.wait_closed()
