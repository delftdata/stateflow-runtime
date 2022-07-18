from universalis.common.stateful_function import StatefulFunction


class Insert(StatefulFunction):
    async def run(self, key: str, stock: dict[str, str]):
        await self.put(key, stock)
        return key, stock
