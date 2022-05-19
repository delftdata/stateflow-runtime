from universalis.common.operator import StatefulFunction


class CreateItem(StatefulFunction):
    async def run(self, key: str, name: str, price: int):
        await self.state.put(key, {'name': name, 'price': price, 'stock': 0})
        return key


class AddStock(StatefulFunction):
    async def run(self, key: str, stock: int):
        item_data = await self.state.get(key)
        item_data['stock'] += stock
        await self.state.put(key, item_data)
        return item_data


class SubtractStock(StatefulFunction):
    async def run(self, key: str, stock: int):
        # is a lock needed here? (we don't want it, the system should handle it)
        item_data = await self.state.get(key)
        item_data['stock'] -= stock
        await self.state.put(key, item_data)
        return item_data
