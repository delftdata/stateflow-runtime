from universalis.common.stateful_function import StatefulFunction


class InsertOrder(StatefulFunction):
    async def run(self, key: str, order: dict[str, int | float | str]):
        await self.put(key, order)
        return key, order


class GetOrder(StatefulFunction):
    async def run(self, key: str):
        order = await self.get(key)
        return order
