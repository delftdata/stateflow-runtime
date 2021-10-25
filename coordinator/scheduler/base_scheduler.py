from abc import ABC, abstractmethod

from universalis.common.stateflow_graph import StateflowGraph
from universalis.common.stateflow_ingress import StateflowIngress
from universalis.common.stateflow_worker import StateflowWorker


class BaseScheduler(ABC):

    @abstractmethod
    async def schedule(self,
                       workers: list[StateflowWorker],
                       ingress: StateflowIngress,
                       execution_graph: StateflowGraph):
        raise NotImplementedError
