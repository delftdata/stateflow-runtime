from universalis.common.stateful_function import StatefulFunction


class Insert(StatefulFunction):
    async def run(self, key: str, order: dict[str, int | float | str]):
        await self.put(key, order)
        return key, order


class Get(StatefulFunction):
    async def run(self, key: str):
        order = await self.get(key)
        return order
