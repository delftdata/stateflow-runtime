from universalis.common.stateful_function import StatefulFunction


class Insert(StatefulFunction):
    async def run(self, key: str, value: str):
        await self.put(key, value)
        # return key


class Read(StatefulFunction):
    async def run(self, key: str):
        data = await self.get(key)
        return data


class Update(StatefulFunction):
    async def run(self, key: str, new_value: str):
        await self.put(key, new_value)
        return key
