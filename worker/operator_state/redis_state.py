import asyncio

import redis.asyncio as redis

from universalis.common.logging import logging
from universalis.common.base_state import BaseOperatorState
from universalis.common.serialization import msgpack_serialization, msgpack_deserialization


class ReadUncommitedException(Exception):
    pass


class RedisOperatorState(BaseOperatorState):

    def __init__(self, operator_names: set[str]):
        super().__init__(operator_names)
        self.redis_connections: dict[str, redis.Redis] = {}
        self.writing_to_db_locks: dict[str, asyncio.Lock] = {}
        for i, operator_name in enumerate(operator_names):
            self.redis_connections[operator_name] = redis.Redis(unix_socket_path='/tmp/redis.sock', db=i)
            self.writing_to_db_locks[operator_name] = asyncio.Lock()

    async def put(self, key, value, t_id: int, operator_name: str):
        value = msgpack_serialization(value)
        await super().put(key, value, t_id, operator_name)

    async def get(self, key, t_id: int, operator_name: str):
        logging.info(f'GET: {key} with t_id: {t_id} operator: {operator_name}')
        async with self.read_set_locks[operator_name]:
            if t_id in self.read_sets[operator_name]:
                self.read_sets[operator_name][t_id].add(key)
            else:
                self.read_sets[operator_name][t_id] = {key}
        async with self.writing_to_db_locks[operator_name]:
            db_value = await self.redis_connections[operator_name].get(key)
        if db_value is None:
            raise ReadUncommitedException(f'Read uncommitted or does not exit in DB of key: {key}')
        else:
            value = msgpack_deserialization(db_value)
            return value

    async def delete(self, key, operator_name: str):
        # Need to find a way to implement deletes
        pass

    async def exists(self, key, operator_name: str):
        return True if await self.redis_connections[operator_name].exists(key) > 0 else False

    async def commit(self, aborted_from_remote: set[int]):
        self.aborted_transactions: set[int] = self.aborted_transactions.union(aborted_from_remote)
        committed_t_ids = set()
        committed_operator_t_ids = await asyncio.gather(*[self.commit_operator(operator_name)
                                                          for operator_name in self.write_sets.keys()])
        for t_ids in committed_operator_t_ids:
            committed_t_ids = committed_t_ids.union(t_ids)
        self.cleanup()
        return committed_t_ids

    async def commit_operator(self, operator_name: str):
        updates_to_commit = {}
        committed_t_ids = set()
        if len(self.write_sets[operator_name]) == 0:
            return committed_t_ids
        for t_id, ws in self.write_sets[operator_name].items():
            if t_id not in self.aborted_transactions:
                updates_to_commit.update(ws)
                committed_t_ids.add(t_id)
        if updates_to_commit:
            logging.info(f'Committing: {updates_to_commit}')
            async with self.writing_to_db_locks[operator_name]:
                await self.redis_connections[operator_name].mset(updates_to_commit)
        return committed_t_ids
