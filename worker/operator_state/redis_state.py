import asyncio

import redis.asyncio as redis

from universalis.common.logging import logging
from universalis.common.base_state import BaseOperatorState, ReadUncommitedException
from universalis.common.serialization import msgpack_serialization, msgpack_deserialization, pickle_deserialization


class RedisOperatorState(BaseOperatorState):

    def __init__(self, operator_names: set[str]):
        super().__init__(operator_names)
        self.redis_connections: dict[str, redis.Redis] = {}
        self.writing_to_db_locks: dict[str, asyncio.Lock] = {}
        for i, operator_name in enumerate(operator_names):
            self.redis_connections[operator_name] = redis.Redis(unix_socket_path='/tmp/redis.sock', db=i)
            self.writing_to_db_locks[operator_name] = asyncio.Lock()

    async def commit_fallback_transaction(self, t_id: int):
        if t_id in self.fallback_commit_buffer:
            for operator_name, kv_pairs in self.fallback_commit_buffer[t_id].items():
                serialized_kv_pairs = {key: msgpack_serialization(value) for key, value in kv_pairs.items()}
                async with self.fallback_commit_buffer_locks[operator_name]:
                    await self.redis_connections[operator_name].mset(serialized_kv_pairs)

    async def get(self, key, t_id: int, operator_name: str):
        # logging.warning(f'GET: {key} with t_id: {t_id} operator: {operator_name}')
        async with self.read_set_locks[operator_name]:
            if t_id in self.read_sets[operator_name]:
                self.read_sets[operator_name][t_id].add(key)
            else:
                self.read_sets[operator_name][t_id] = {key}
        # if transaction wrote read from the write set
        if t_id in self.write_sets[operator_name] and key in self.write_sets[operator_name][t_id]:
            self.reads[operator_name][key] = min(self.reads[operator_name].get(key, t_id), t_id)
            return self.write_sets[operator_name][t_id][key]

        async with self.writing_to_db_locks[operator_name]:
            db_value = await self.redis_connections[operator_name].get(key)
        if db_value is None:
            if t_id in self.write_sets[operator_name] and key in self.write_sets[operator_name][t_id]:
                return self.write_sets[operator_name][t_id][key]
            else:
                raise ReadUncommitedException(f'Read uncommitted or does not exit in DB of key: {key}')
        else:
            self.reads[operator_name][key] = min(self.reads[operator_name].get(key, t_id), t_id)
            value = msgpack_deserialization(db_value)
            return value

    async def delete(self, key, operator_name: str):
        # Need to find a way to implement deletes
        pass

    async def exists(self, key, operator_name: str):
        return True if await self.redis_connections[operator_name].exists(key) > 0 else False

    async def commit(self, aborted_from_remote: set[int]) -> set[int]:
        self.aborted_transactions: set[int] = self.aborted_transactions.union(aborted_from_remote)
        committed_t_ids = set()
        committed_operator_t_ids = await asyncio.gather(*[self.commit_operator(operator_name)
                                                          for operator_name in self.write_sets.keys()])
        for t_ids in committed_operator_t_ids:
            committed_t_ids = committed_t_ids.union(t_ids)
        return committed_t_ids

    async def commit_operator(self, operator_name: str) -> set[int]:
        updates_to_commit = {}
        committed_t_ids = set()
        if len(self.write_sets[operator_name]) == 0:
            return committed_t_ids
        for t_id, ws in self.write_sets[operator_name].items():
            if t_id not in self.aborted_transactions:
                updates_to_commit.update(ws)
                committed_t_ids.add(t_id)
        if updates_to_commit:
            # deser = {key: pickle_deserialization(value) for key, value in updates_to_commit.items()}
            # logging.warning(f'Committing: {deser}')
            serialized_kv_pairs = {key: msgpack_serialization(value) for key, value in updates_to_commit.items()}
            async with self.writing_to_db_locks[operator_name]:
                await self.redis_connections[operator_name].mset(serialized_kv_pairs)
        return committed_t_ids
