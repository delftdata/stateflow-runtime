from universalis.common.stateful_function import StatefulFunction


class Insert(StatefulFunction):
    async def run(self, key: str, history: dict[str, int | str | float]):
        await self.put(key, history)
        return key, history
