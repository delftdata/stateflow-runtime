from threading import Lock
from typing import Any
# import logging
# logging.basicConfig(level=logging.INFO)


class OperatorState:

    def __init__(self):
        self.data: dict = {}
        self.key_locks: dict[str, Lock] = {}

    def create(self, key: str, value: object):
        self.key_locks[key] = Lock()
        self.key_locks[key].acquire()
        try:
            self.data[key] = value
        finally:
            self.key_locks[key].release()
            # logging.info(f'State: {self.data}')

    def read(self, key: str) -> Any:
        self.key_locks[key].acquire()
        try:
            value = self.data[key]
        finally:
            self.key_locks[key].release()
            # logging.info(f'State: {self.data}')
        return value

    def update(self, key: str, new_value: object):
        self.key_locks[key].acquire()
        try:
            self.data[key] = new_value
        finally:
            self.key_locks[key].release()
            # logging.info(f'State: {self.data}')

    def delete(self, key: str):
        self.key_locks[key].acquire()
        try:
            del self.data[key]
        finally:
            self.key_locks[key].release()
