from abc import ABC, abstractmethod


class BaseOperatorState(ABC):

    @abstractmethod
    def put(self, key, value):
        raise NotImplementedError

    @abstractmethod
    def get(self, key):
        raise NotImplementedError

    @abstractmethod
    def delete(self, key):
        raise NotImplementedError
