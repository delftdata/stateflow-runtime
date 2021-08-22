# import time
# import logging
# from timeit import default_timer as timer
#
# from opeartor import Operator, StatefulFunction
# from order import add_item, create_order, checkout
# from demo.stock import create_item, add_stock, subtract_stock
# from user import create_user, add_credit, subtract_credit
#
#
# async def start_scenario(own_address: str, registered_operators):
#     apples_key: str = 'item-key'
#     asterios_key: str = 'user-key'
#     asterios_order_key: str = 'order-key'
#     if own_address == 'worker-0':  # worker with user
#         registered_operators['user'] = Operator()
#         user_operator = registered_operators['user']
#         user_operator.register_stateful_function(StatefulFunction(create_user))
#         user_operator.register_stateful_function(StatefulFunction(add_credit))
#         user_operator.register_stateful_function(StatefulFunction(subtract_credit))
#         create_user_function = user_operator.functions['create_user']
#         add_credit_function = user_operator.functions['add_credit']
#         logging.info('User functions registered')
#         # start logic
#         start = timer()
#         create_user_function(asterios_key, 'Asterios')
#         logging.info('User created')
#         add_credit_function(asterios_key, 10)
#         logging.info('User recieved credit')
#         end = timer()
#         logging.info(f'User logic took {(end - start) * 1000} ms')
#     elif own_address == 'worker-1':  # worker with stock
#         registered_operators['stock'] = Operator()
#         stock_operator = registered_operators['stock']
#         stock_operator.register_stateful_function(StatefulFunction(create_item))
#         stock_operator.register_stateful_function(StatefulFunction(add_stock))
#         stock_operator.register_stateful_function(StatefulFunction(subtract_stock))
#         create_item_function = stock_operator.functions['create_item']
#         add_stock_function = stock_operator.functions['add_stock']
#         logging.info('Stock functions registered')
#         # start logic
#         start = timer()
#         create_item_function(apples_key, 'apples', 2)
#         logging.info('Item created')
#         add_stock_function(apples_key, 9000)
#         logging.info('Item received stock')
#         end = timer()
#         logging.info(f'Stock logic took {(end - start) * 1000} ms')
#     elif own_address == 'worker-2':  # worker with order
#         time.sleep(1)
#         registered_operators['order'] = Operator()
#         order_operator = registered_operators['order']
#         order_operator.register_stateful_function(StatefulFunction(create_order))
#         order_operator.register_stateful_function(StatefulFunction(add_item))
#         order_operator.register_stateful_function(StatefulFunction(checkout))
#         create_order_function = order_operator.functions['create_order']
#         add_item_function = order_operator.functions['add_item']
#         checkout_function = order_operator.functions['checkout']
#         logging.info('Order functions registered')
#         # start logic
#         start = timer()
#         create_order_function(asterios_order_key, asterios_key)
#         logging.info('Order created')
#         add_item_function(asterios_order_key, apples_key, 2, 2)
#         logging.info('Order received items')
#         checkout_function(asterios_order_key)
#         logging.info('Checkout completed')
#         end = timer()
#         logging.info(f'Order logic took {(end - start) * 1000} ms')
