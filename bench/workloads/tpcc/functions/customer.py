from universalis.common.stateful_function import StatefulFunction


class Insert(StatefulFunction):
    async def run(self, key: str, customer: dict[str, int | float | str]):
        await self.put(key, customer)
        return key, customer


class NewOrder(StatefulFunction):
    async def run(self, key: str, params: tuple):
        pass


class Payment(StatefulFunction):
    async def run(self, key: str, params: tuple):
        pass
