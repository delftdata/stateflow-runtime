import asyncio
import time
import cloudpickle

from universalis.common.logging import logging
from universalis.common.networking import async_transmit_tcp_no_response
from universalis.common.stateflow_graph import StateflowGraph
from universalis.common.stateflow_worker import StateflowWorker
from universalis.common.operator import BaseOperator, StatefulFunction


class NotAStateflowGraph(Exception):
    pass


class Universalis:

    def __init__(self, coordinator_adr: str, coordinator_port: int):
        self.coordinator_adr = coordinator_adr
        self.coordinator_port = coordinator_port
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

    async def send_execution_graph(self, stateflow_graph: StateflowGraph):
        _, writer = await asyncio.open_connection(self.coordinator_adr, self.coordinator_port)
        writer.write(cloudpickle.dumps({"__COM_TYPE__": "SEND_EXECUTION_GRAPH",
                                        "__MSG__": stateflow_graph}))
        await writer.drain()
        writer.write_eof()
        writer.close()
        await writer.wait_closed()
