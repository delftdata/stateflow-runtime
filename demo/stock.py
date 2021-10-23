from common.operator import StatefulFunction


class CreateItem(StatefulFunction):
    async def run(self, key: str, name: str, price: int):
        await self.state.create(key, {'name': name, 'price': price, 'stock': 0})
        return key


class AddStock(StatefulFunction):
    async def run(self, key: str, stock: int):
        item_data = await self.state.read(key)
        item_data['stock'] += stock
        await self.state.update(key, item_data)


class SubtractStock(StatefulFunction):
    async def run(self, key: str, stock: int):
        item_data = await self.state.read(key)
        item_data['stock'] -= stock
        await self.state.update(key, item_data)
