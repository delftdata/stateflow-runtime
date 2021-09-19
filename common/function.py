from abc import ABC, abstractmethod

from common.network_client import NetworkTCPClient


class Function(ABC):

    def __init__(self):
        self.name = type(self).__name__
        self.networking = NetworkTCPClient()

    def __call__(self, *args, **kwargs):
        return self.run(*args)

    @abstractmethod
    def run(self, *args):
        raise NotImplementedError
