from scheduler.round_robin import RoundRobin

from universalis.common.stateflow_graph import StateflowGraph
from universalis.common.stateflow_worker import StateflowWorker
# from universalis.common.stateflow_ingress import StateflowIngress


class NotAStateflowGraph(Exception):
    pass


class Coordinator:

    def __init__(self):
        # TODO get workers and ingresses dynamically
        self.workers = [StateflowWorker("worker-0", 8888),
                        StateflowWorker("worker-1", 8888),
                        StateflowWorker("worker-2", 8888)]
        self.ingress = StateflowWorker("ingress-load-balancer", 4000)
        # self.ingress = StateflowIngress("ingress-load-balancer", 4000, "ingress-load-balancer", 4000)

    async def submit_stateflow_graph(self, stateflow_graph: StateflowGraph, scheduler_type=None):
        if not isinstance(stateflow_graph, StateflowGraph):
            raise NotAStateflowGraph
        scheduler = RoundRobin()
        await scheduler.schedule(self.workers, self.ingress, stateflow_graph)
