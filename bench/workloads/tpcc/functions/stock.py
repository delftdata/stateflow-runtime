from universalis.common.stateful_function import StatefulFunction


class InsertStock(StatefulFunction):
    async def run(self, key: str, stock: dict[str, str]):
        await self.put(key, stock)
        return key, stock


class GetStock(StatefulFunction):
    async def run(self, key: str):
        stock = await self.get(key)
        return stock
