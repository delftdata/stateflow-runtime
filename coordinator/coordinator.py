from coordinator.scheduler.round_robin import RoundRobin
from coordinator.stateflow_graph import StateflowGraph
from coordinator.worker import StateflowWorker


class NotAStateflowGraph(Exception):
    pass


class Coordinator:

    def __init__(self, host: str, port: int):
        self.own_host = host
        self.own_port = port
        self.workers = [StateflowWorker("worker-0", 8888),
                        StateflowWorker("worker-1", 8888),
                        StateflowWorker("worker-2", 8888)]
        self.operators = {}
        self.source = None
        self.sink = None

    def submit_stateflow_graph(self, stateflow_graph: StateflowGraph, scheduler_type=None):
        if not isinstance(stateflow_graph, StateflowGraph):
            raise NotAStateflowGraph
        scheduler = RoundRobin()
        scheduler.schedule(self.workers, stateflow_graph)
