from abc import ABC, abstractmethod


class BaseOperatorState(ABC):

    @abstractmethod
    async def put(self, key, value):
        raise NotImplementedError

    @abstractmethod
    async def get(self, key):
        raise NotImplementedError

    @abstractmethod
    async def delete(self, key):
        raise NotImplementedError

    @abstractmethod
    async def exists(self, key):
        raise NotImplementedError
