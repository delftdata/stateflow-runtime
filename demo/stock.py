from universalis_operator.opeartor import StatefulFunction


class CreateItem(StatefulFunction):
    def run(self, key: str, name: str, price: int):
        self.state.create(key, {'name': name, 'price': price, 'stock': 0})
        return key


class AddStock(StatefulFunction):
    def run(self, key: str, stock: int):
        item_data = self.state.read(key)
        item_data['stock'] += stock
        self.state.update(key, item_data)


class SubtractStock(StatefulFunction):
    def run(self, key: str, stock: int):
        item_data = self.state.read(key)
        item_data['stock'] -= stock
        self.state.update(key, item_data)
