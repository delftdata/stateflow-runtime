from universalis.common.stateful_function import StatefulFunction


class Insert(StatefulFunction):
    async def run(self, key: str, order_line: dict[str, str | None | int | float]):
        await self.put(key, order_line)
        return key, order_line
