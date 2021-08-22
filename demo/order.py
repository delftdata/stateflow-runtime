from universalis_operator.opeartor import StatefulFunction


class CreateOrder(StatefulFunction):
    def run(self, key, user_key):
        self.state.create(key, {'user_key': user_key, 'items': []})
        return key


class AddItem(StatefulFunction):
    def run(self, order_key, item_key, quantity, cost):
        order_data = self.state.read(order_key)
        order_data['items'].append({'item_key': item_key, 'quantity': quantity, 'cost': cost})
        self.state.update(order_key, order_data)


class Checkout(StatefulFunction):
    def run(self, order_key):
        order_data = self.state.read(order_key)
        total_cost = 0
        for item in order_data['items']:
            # call stock operator here to subtract stock
            self.networking.send_request_to_other_operator('stock', 'subtract_stock', {'item_key': item['item_key'],
                                                                                       'quantity': item['quantity']})
            total_cost += item['quantity'] * item['cost']
        # call user operator to subtract credit
        self.networking.send_request_to_other_operator('user', 'subtract_credit', {'user_key': order_data['user_key'],
                                                                                   'credit': total_cost})
