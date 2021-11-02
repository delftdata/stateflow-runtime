import aioredis
from universalis.common.base_state import BaseOperatorState
from universalis.common.serialization import msgpack_serialization, msgpack_deserialization


class RedisOperatorState(BaseOperatorState):

    def __init__(self):
        self.local_redis_instance = aioredis.from_url('unix:///tmp/redis.sock')

    async def put(self, key, value):
        await self.local_redis_instance.set(key, msgpack_serialization(value))

    async def get(self, key):
        return msgpack_deserialization(await self.local_redis_instance.get(key))

    async def delete(self, key):
        await self.local_redis_instance.delete([key])
