from universalis.common.stateful_function import StatefulFunction


class Insert(StatefulFunction):
    async def run(self, key: str, item: dict[str, int | float | str]):
        await self.put(key, item)
        return key, item


class Get(StatefulFunction):
    async def run(self, key: str):
        item = await self.get(key)
        return item
