from universalis.common.stateflow_graph import StateflowGraph
from universalis.common.stateflow_worker import StateflowWorker

from scheduler.round_robin import RoundRobin


class NotAStateflowGraph(Exception):
    pass


class Coordinator:

    def __init__(self):
        # TODO get workers and ingresses dynamically
        self.workers = [StateflowWorker("worker-0", 8888),
                        StateflowWorker("worker-1", 8888),
                        StateflowWorker("worker-2", 8888)]

    async def submit_stateflow_graph(self,
                                     network_manager,
                                     stateflow_graph: StateflowGraph,
                                     scheduler_type=None) -> dict[dict[str, tuple[str, int]]]:
        if not isinstance(stateflow_graph, StateflowGraph):
            raise NotAStateflowGraph
        scheduler = RoundRobin()
        return await scheduler.schedule(self.workers, stateflow_graph, network_manager)
