from universalis.common.operator import StatefulFunction
from universalis.common.logging import logging


class CreateOrder(StatefulFunction):
    async def run(self, key, user_key):
        await self.state.put(key, {'user_key': user_key, 'items': []})
        # return key


class AddItem(StatefulFunction):
    async def run(self, order_key, item_key, quantity, cost):
        order_data = await self.state.get(order_key)
        order_data['items'].append({'item_key': item_key, 'quantity': quantity, 'cost': cost})
        await self.state.put(order_key, order_data)


class Checkout(StatefulFunction):
    async def run(self, order_key):
        order_data = await self.state.get(order_key)
        total_cost = 0
        for item in order_data['items']:
            # call stock operator here to subtract stock
            logging.warning('(O)  REQUEST -> SubtractStock')

            res = await self.call_remote_function_request_response(operator_name='stock',
                                                                   function_name='SubtractStock',
                                                                   key=item['item_key'],
                                                                   params=(item['item_key'], item['quantity']))
            logging.warning(f'(O)  RESPONSE -> {res}')

            total_cost += item['quantity'] * item['cost']
            # call user operator to subtract credit
            self.call_remote_function_no_response(operator_name='user',
                                                  function_name='SubtractCredit',
                                                  key=order_data['user_key'],
                                                  params=(order_data['user_key'], total_cost))
