from asyncio import Lock
from typing import Any

from universalis.common.logging import logging
from universalis.common.base_state import BaseOperatorState


class InMemoryOperatorState(BaseOperatorState):

    def __init__(self):
        self.data: dict = {}
        self.key_locks: dict[str, Lock] = {}

    async def put(self, key, value):
        self.key_locks[key] = Lock()
        await self.key_locks[key].acquire()
        try:
            self.data[key] = value
        finally:
            self.key_locks[key].release()
            logging.debug(f'State: {self.data}')

    async def get(self, key) -> Any:
        try:
            await self.key_locks[key].acquire()
            try:
                value = self.data[key]
            finally:
                self.key_locks[key].release()
                logging.debug(f'State: {self.data}')
            return value
        except KeyError:
            logging.warning(f'Key: {key} does not exist')

    async def delete(self, key: str):
        try:
            await self.key_locks[key].acquire()
            try:
                del self.data[key]
            finally:
                self.key_locks[key].release()
        except KeyError:
            logging.warning(f'Key: {key} does not exist')
