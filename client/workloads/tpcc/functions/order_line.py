from universalis.common.stateful_function import StatefulFunction


class InsertOrderLine(StatefulFunction):
    async def run(self, key: str, order_line: dict[str, str | None | int | float]):
        await self.put(key, order_line)
        return key, order_line


class GetOrderLine(StatefulFunction):
    async def run(self, key: str):
        order_line = await self.get(key)
        return order_line
