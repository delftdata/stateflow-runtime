from universalis.common.operator import StatefulFunction


class NotEnoughStock(Exception):
    pass


class CreateItem(StatefulFunction):
    async def run(self, key: str, name: str, price: int):
        await self.put(key, {'name': name, 'price': price, 'stock': 0})
        return key


class AddStock(StatefulFunction):
    async def run(self, key: str, stock: int):
        item_data = await self.get(key)
        item_data['stock'] += stock
        await self.put(key, item_data)


class SubtractStock(StatefulFunction):
    async def run(self, key: str, stock: int):
        item_data = await self.get(key)
        item_data['stock'] -= stock
        if item_data['stock'] < 0:
            raise NotEnoughStock()
        await self.put(key, item_data)
