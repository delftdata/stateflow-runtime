import asyncio
import socket
import struct
import time
import cloudpickle
from universalis.common.serialization import msgpack_serialization
from websockets import connect

from universalis.common.logging import logging
from universalis.common.networking import async_transmit_tcp_no_response, \
    async_transmit_websocket_no_response_new_connection
from universalis.common.stateflow_graph import StateflowGraph
from universalis.common.stateflow_worker import StateflowWorker
from universalis.common.operator import BaseOperator, StatefulFunction


class NetworkingManager:

    def __init__(self):
        self.open_socket_connections: dict[tuple[str, int], socket.socket] = {}

    def create_socket_connection(self, host: str, port):
        s = None
        for res in socket.getaddrinfo(host, port, socket.AF_UNSPEC, socket.SOCK_STREAM):
            af, socktype, proto, canonname, sa = res
            try:
                s = socket.socket(af, socktype, proto)
            except OSError:
                s = None
                continue
            try:
                s.connect(sa)
            except OSError:
                s.close()
                s = None
                continue
            break
        if s is None:
            logging.error(f'Could not open socket for host: {host}:{port}')
        else:
            self.open_socket_connections[(host, port)] = s

    def close_socket_connection(self, host: str, port: int):
        if (host, port) in self.open_socket_connections:
            self.open_socket_connections[(host, port)].close()

    def send_message(self, host, port, msg: bytes):
        sock = self.open_socket_connections[(host, port)]
        # Prefix each message with a 4-byte length (network byte order)
        msg = struct.pack('>I', len(msg)) + msg
        try:
            sock.sendall(msg)
        except BrokenPipeError:
            self.close_socket_connection(host, port)
            self.create_socket_connection(host, port)
            self.open_socket_connections[(host, port)].sendall(msg)

    def receive_message(self, host, port):
        sock = self.open_socket_connections[(host, port)]
        # Read message length and unpack it into an integer
        raw_message_len = self.__receive_all(sock, 4)
        if not raw_message_len:
            return None
        message_len = struct.unpack('>I', raw_message_len)[0]
        # Read the message data
        return self.__receive_all(sock, message_len)

    @staticmethod
    def __receive_all(sock, n):
        # Helper function to receive n bytes or return None if EOF is hit
        data = bytearray()
        while len(data) < n:
            packet = sock.recv(n - len(data))
            if not packet:
                return None
            data.extend(packet)
        return data


class NotAStateflowGraph(Exception):
    pass


class Universalis:

    def __init__(self, coordinator_adr: str, coordinator_port: int):
        self.coordinator_adr = coordinator_adr
        self.coordinator_port = coordinator_port
        self.networking_manager = NetworkingManager()
        self.networking_manager.create_socket_connection('ingress-load-balancer', 4000)
        self.networking_manager.create_socket_connection(self.coordinator_adr, self.coordinator_port)
        self.ingress_that_serves: StateflowWorker = StateflowWorker('ingress-load-balancer', 4000)

    def submit(self, stateflow_graph: StateflowGraph, *modules):
        logging.info(f'Submitting Stateflow graph: {stateflow_graph.name}')
        if not isinstance(stateflow_graph, StateflowGraph):
            raise NotAStateflowGraph
        for module in modules:
            cloudpickle.register_pickle_by_value(module)
        asyncio.run(self.send_execution_graph(stateflow_graph))
        logging.info(f'Submission of Stateflow graph: {stateflow_graph.name} completed')
        time.sleep(0.05)  # Sleep for 50ms to allow for the graph to setup

    def submit_websockets(self, stateflow_graph: StateflowGraph, *modules):
        logging.info(f'Submitting Stateflow graph: {stateflow_graph.name}')
        if not isinstance(stateflow_graph, StateflowGraph):
            raise NotAStateflowGraph
        for module in modules:
            cloudpickle.register_pickle_by_value(module)
        asyncio.run(self.send_execution_graph_websockets(stateflow_graph))
        logging.info(f'Submission of Stateflow graph: {stateflow_graph.name} completed')
        time.sleep(0.05)  # Sleep for 50ms to allow for the graph to setup

    def submit_protocol(self, stateflow_graph: StateflowGraph, *modules):
        logging.info(f'Submitting Stateflow graph: {stateflow_graph.name}')
        if not isinstance(stateflow_graph, StateflowGraph):
            raise NotAStateflowGraph
        for module in modules:
            cloudpickle.register_pickle_by_value(module)
        self.send_execution_graph_protocol(stateflow_graph)
        logging.info(f'Submission of Stateflow graph: {stateflow_graph.name} completed')
        time.sleep(0.05)  # Sleep for 50ms to allow for the graph to setup

    def send_tcp_event(self,
                       operator: BaseOperator,
                       key,
                       function: StatefulFunction,
                       params: tuple,
                       timestamp: int = None):
        if timestamp is None:
            timestamp = time.time_ns()
        event = {'__OP_NAME__': operator.name,
                 '__KEY__': key,
                 '__FUN_NAME__': function.name,
                 '__PARAMS__': params,
                 '__TIMESTAMP__': timestamp}

        asyncio.run(async_transmit_tcp_no_response(self.ingress_that_serves.host,
                                                   self.ingress_that_serves.port,
                                                   event,
                                                   com_type='REMOTE_FUN_CALL'))

    def send_websocket_event(self,
                             operator: BaseOperator,
                             key,
                             function: StatefulFunction,
                             params: tuple,
                             timestamp: int = None):
        if timestamp is None:
            timestamp = time.time_ns()
        event = {'__OP_NAME__': operator.name,
                 '__KEY__': key,
                 '__FUN_NAME__': function.name,
                 '__PARAMS__': params,
                 '__TIMESTAMP__': timestamp}

        asyncio.run(async_transmit_websocket_no_response_new_connection(self.ingress_that_serves.host,
                                                                        self.ingress_that_serves.port,
                                                                        event,
                                                                        com_type='REMOTE_FUN_CALL'))

    def send_protocol_event(self,
                            operator: BaseOperator,
                            key,
                            function: StatefulFunction,
                            params: tuple,
                            timestamp: int = None):
        if timestamp is None:
            timestamp = time.time_ns()
        event = {'__OP_NAME__': operator.name,
                 '__KEY__': key,
                 '__FUN_NAME__': function.name,
                 '__PARAMS__': params,
                 '__TIMESTAMP__': timestamp}

        self.networking_manager.send_message(self.ingress_that_serves.host,
                                             self.ingress_that_serves.port,
                                             msgpack_serialization({"__COM_TYPE__": 'REMOTE_FUN_CALL',
                                                                    "__MSG__": event}))

    async def send_execution_graph(self, stateflow_graph: StateflowGraph):
        _, writer = await asyncio.open_connection(self.coordinator_adr, self.coordinator_port)
        writer.write(cloudpickle.dumps({"__COM_TYPE__": "SEND_EXECUTION_GRAPH",
                                        "__MSG__": stateflow_graph}))
        await writer.drain()
        writer.write_eof()
        writer.close()
        await writer.wait_closed()

    async def send_execution_graph_websockets(self, stateflow_graph: StateflowGraph):
        async with connect(f"ws://{self.coordinator_adr}:{self.coordinator_port}") as websocket:
            serialized_message = cloudpickle.dumps({"__COM_TYPE__": "SEND_EXECUTION_GRAPH",
                                                    "__MSG__": stateflow_graph})
            await websocket.send(serialized_message)

    def send_execution_graph_protocol(self, stateflow_graph: StateflowGraph):
        self.networking_manager.send_message(self.coordinator_adr,
                                             self.coordinator_port,
                                             cloudpickle.dumps({"__COM_TYPE__": 'SEND_EXECUTION_GRAPH',
                                                                "__MSG__": stateflow_graph}))


class AsyncUniversalis:

    def __init__(self, coordinator_adr: str, coordinator_port: int):
        self.coordinator_adr = coordinator_adr
        self.coordinator_port = coordinator_port
        self.ingress_that_serves: StateflowWorker = StateflowWorker('ingress-load-balancer', 4000)

    async def submit(self, stateflow_graph: StateflowGraph, *modules):
        logging.info(f'Submitting Stateflow graph: {stateflow_graph.name}')
        if not isinstance(stateflow_graph, StateflowGraph):
            raise NotAStateflowGraph
        for module in modules:
            cloudpickle.register_pickle_by_value(module)
        await self.send_execution_graph(stateflow_graph)
        logging.info(f'Submission of Stateflow graph: {stateflow_graph.name} completed')
        time.sleep(0.05)  # Sleep for 50ms to allow for the graph to setup

    async def send_tcp_event(self,
                             operator: BaseOperator,
                             key,
                             function: StatefulFunction,
                             params: tuple,
                             timestamp: int = None):
        if timestamp is None:
            timestamp = time.time_ns()
        event = {'__OP_NAME__': operator.name,
                 '__KEY__': key,
                 '__FUN_NAME__': function.name,
                 '__PARAMS__': params,
                 '__TIMESTAMP__': timestamp}

        await async_transmit_tcp_no_response(self.ingress_that_serves.host,
                                             self.ingress_that_serves.port,
                                             event,
                                             com_type='REMOTE_FUN_CALL')

    async def send_websocket_event(self,
                                   operator: BaseOperator,
                                   key,
                                   function: StatefulFunction,
                                   params: tuple,
                                   timestamp: int = None):
        if timestamp is None:
            timestamp = time.time_ns()
        event = {'__OP_NAME__': operator.name,
                 '__KEY__': key,
                 '__FUN_NAME__': function.name,
                 '__PARAMS__': params,
                 '__TIMESTAMP__': timestamp}

        await async_transmit_websocket_no_response_new_connection(self.ingress_that_serves.host,
                                                                  self.ingress_that_serves.port,
                                                                  event,
                                                                  com_type='REMOTE_FUN_CALL')

    async def send_execution_graph(self, stateflow_graph: StateflowGraph):
        _, writer = await asyncio.open_connection(self.coordinator_adr, self.coordinator_port)
        writer.write(cloudpickle.dumps({"__COM_TYPE__": "SEND_EXECUTION_GRAPH",
                                        "__MSG__": stateflow_graph}))
        await writer.drain()
        writer.write_eof()
        writer.close()
        await writer.wait_closed()

    async def send_execution_graph_websockets(self, stateflow_graph: StateflowGraph):
        async with connect(f"ws://{self.coordinator_adr}:{self.coordinator_port}") as websocket:
            serialized_message = cloudpickle.dumps({"__COM_TYPE__": "SEND_EXECUTION_GRAPH",
                                                    "__MSG__": stateflow_graph})
            await websocket.send(serialized_message)
