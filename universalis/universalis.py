import asyncio
import time
import cloudpickle

from common.logging import logging
from common.stateflow_graph import StateflowGraph
from common.stateflow_worker import StateflowWorker
from common.network_client import NetworkTCPClient
from common.opeartor import Operator, StatefulFunction


class NotAStateflowGraph(Exception):
    pass


class Universalis:

    def __init__(self, coordinator_adr: str, coordinator_port: int):
        self.coordinator_adr = coordinator_adr
        self.coordinator_port = coordinator_port
        self.ingress_that_serves: StateflowWorker = StateflowWorker('', -1)

    def submit(self, stateflow_graph: StateflowGraph, *modules):
        logging.info(f'Submitting Stateflow graph: {stateflow_graph.name}')
        if not isinstance(stateflow_graph, StateflowGraph):
            raise NotAStateflowGraph
        for module in modules:
            cloudpickle.register_pickle_by_value(module)
        self.ingress_that_serves = StateflowWorker(*asyncio.run(self.async_send_execution_graph(stateflow_graph)))
        time.sleep(1)  # TODO remove the sleep
        logging.info(f'Submission of Stateflow graph: {stateflow_graph.name} completed')

    def send_tcp_event(self, operator: Operator, function: StatefulFunction, *params, timestamp: int = None):
        ingress = NetworkTCPClient(self.ingress_that_serves.host, self.ingress_that_serves.port)
        if timestamp is None:
            timestamp = time.time_ns()
        event = {'__OP_NAME__': operator.name,
                 '__FUN_NAME__': function.name,
                 '__PARAMS__': params,
                 '__TIMESTAMP__': timestamp}
        ingress.transmit_tcp_no_response(event, com_type='REMOTE_FUN_CALL')

    async def async_send_execution_graph(self, stateflow_graph: StateflowGraph) -> object:
        reader, writer = await asyncio.open_connection(self.coordinator_adr, self.coordinator_port)
        writer.write(cloudpickle.dumps({"__COM_TYPE__": "SEND_EXECUTION_GRAPH", "__MSG__": stateflow_graph}))
        data = await reader.read()
        writer.close()
        return cloudpickle.loads(data)
