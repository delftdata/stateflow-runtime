from asyncio import Lock
from typing import Any

from common.logging import logging


class OperatorState:

    def __init__(self):
        self.data: dict = {}
        self.key_locks: dict[str, Lock] = {}

    async def create(self, key: str, value: object):
        self.key_locks[key] = Lock()
        await self.key_locks[key].acquire()
        try:
            self.data[key] = value
        finally:
            self.key_locks[key].release()
            logging.debug(f'State: {self.data}')

    async def read(self, key: str) -> Any:
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

    async def update(self, key: str, new_value: object):
        try:
            await self.key_locks[key].acquire()
            try:
                self.data[key] = new_value
            finally:
                self.key_locks[key].release()
                logging.debug(f'State: {self.data}')
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
