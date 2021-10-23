from scheduler.round_robin import RoundRobin

from common.stateflow_graph import StateflowGraph
from common.stateflow_worker import StateflowWorker
from common.stateflow_ingress import StateflowIngress
from common.logging import logging


class NotAStateflowGraph(Exception):
    pass


class Coordinator:

    def __init__(self):
        # TODO get workers and ingresses dynamically
        self.workers = [StateflowWorker("worker-0", 8888),
                        StateflowWorker("worker-1", 8888),
                        StateflowWorker("worker-2", 8888)]
        # self.ingresses = [StateflowIngress("ingress-0", 8888, '0.0.0.0', 8885)]
        self.ingresses = [StateflowIngress("nginx", 4000, "nginx", 4000)]

    async def submit_stateflow_graph(self, stateflow_graph: StateflowGraph, scheduler_type=None):
        if not isinstance(stateflow_graph, StateflowGraph):
            raise NotAStateflowGraph
        scheduler = RoundRobin()
        ingress_that_serves_host, ingress_that_serves_port = await scheduler.schedule(self.workers,
                                                                                      self.ingresses,
                                                                                      stateflow_graph)
        return ingress_that_serves_host, ingress_that_serves_port
