from abc import ABC, abstractmethod
from typing import Union, Callable
from uuid import uuid4
from timeit import default_timer as timer

from .network_client import NetworkTCPClient
from .state import OperatorState


class StateNotAttachedError(Exception):
    pass


class NotAFunctionError(Exception):
    pass


class Function:

    def __init__(self, python_function):
        self.name = python_function.__name__
        self.__run = python_function

    def __call__(self, *args, **kwargs):
        return self.__run(*args)

    def __run(self):
        raise NotImplementedError


class StatefulFunction(ABC):

    state: OperatorState

    def __init__(self):
        self.name = type(self).__name__
        self.networking = NetworkTCPClient()

    def __call__(self, *args, **kwargs):
        if self.state is None:
            raise StateNotAttachedError('Cannot call stateful function without attached state')
        return self.run(self, args)

    def attach_state(self, operator_state: OperatorState):
        self.state = operator_state

    @abstractmethod
    def run(self, *args):
        raise NotImplementedError


class Operator:
    # each operator is a coroutine? and a server. Client is already implemented in the networking module
    def __init__(self, name: str):
        self.name = name
        self.state: OperatorState = OperatorState()
        self.functions: dict[str, Union[Function, StatefulFunction]] = {}
        self.dns = {}  # where the other functions exist

    def register_function(self, function: Function):
        self.functions[function.name] = function

    def register_stateful_function(self, function: StatefulFunction):
        if StatefulFunction not in type(function).__bases__:
            raise NotAFunctionError
        stateful_function = function
        self.functions[stateful_function.name] = stateful_function
        self.functions[stateful_function.name].attach_state(self.state)

    def register_stateful_functions(self, *functions: StatefulFunction):
        for function in functions:
            self.register_stateful_function(function)

    def call_remote_function(self):
        pass


# if __name__ == '__main__':
#
#     def create_user(self: StatefulFunction, name: str):
#         key = str(uuid4())
#         self.state.create(key, {'name': name, 'credit': 0})
#         return key
#
#     def add_credit(self: StatefulFunction, key: str, credit: int):
#         user_data = self.state.read(key)
#         user_data['credit'] += credit
#         self.state.update(key, user_data)
#
#     def subtract_credit(self: StatefulFunction, key: str, credit: int):
#         user_data = self.state.read(key)
#         user_data['credit'] -= credit
#         self.state.update(key, user_data)
#
#     def create_item(self: StatefulFunction, name: str, price: int):
#         key = str(uuid4())
#         self.state.create(key, {'name': name, 'price': price, 'stock': 0})
#         return key
#
#     def add_stock(self: StatefulFunction, key: str, stock: int):
#         item_data = self.state.read(key)
#         item_data['stock'] += stock
#         self.state.update(key, item_data)
#
#     def subtract_stock(self: StatefulFunction, key: str, stock: int):
#         item_data = self.state.read(key)
#         item_data['stock'] -= stock
#         self.state.update(key, item_data)
#
#     def create_order(self: StatefulFunction, user_key: str):
#         key = str(uuid4())
#         self.state.create(key, {'user_key': user_key, 'items': []})
#         return key
#
#     def add_item(self: StatefulFunction, order_key: str, item_key: str, quantity: int):
#         order_data = self.state.read(order_key)
#         order_data['items'].append({'item_key': item_key, 'quantity': quantity})
#         self.state.update(order_key, order_data)
#
#     def checkout(self: StatefulFunction, order_key: str):
#         order_data = self.state.read(order_key)
#         for item in order_data['items']:
#             pass  # call stock operator here to subtract stock
#         pass  # call user operator here to subtract credit
#
#     start = timer()
#     # ------------------------------------------------------------------------
#     # USER
#     user_operator = Operator()
#     user_operator.register_stateful_function(StatefulFunction(create_user))
#     user_operator.register_stateful_function(StatefulFunction(add_credit))
#     create_user_function = user_operator.functions['create_user']
#     add_credit_function = user_operator.functions['add_credit']
#     asterios_key = create_user_function('Asterios')
#     add_credit_function(asterios_key, 10)
#     # STOCK
#     stock_operator = Operator()
#     stock_operator.register_stateful_function(StatefulFunction(create_item))
#     stock_operator.register_stateful_function(StatefulFunction(add_stock))
#     create_item_function = stock_operator.functions['create_item']
#     add_stock_function = stock_operator.functions['add_stock']
#     apples_key = create_item_function('apples', 2)
#     add_stock_function(apples_key, 9000)
#     # ORDER
#     order_operator = Operator()
#     order_operator.register_stateful_function(StatefulFunction(create_order))
#     order_operator.register_stateful_function(StatefulFunction(add_item))
#     create_order_function = order_operator.functions['create_order']
#     add_item_function = order_operator.functions['add_item']
#     asterios_order_key = create_order_function(asterios_key)
#     add_item_function(asterios_order_key, apples_key, 2)
#     # ------------------------------------------------------------------------
#     end = timer()
#     print(f'Elapsed time: {(end - start) * 1000} ms')
#     print(f'User operator state: {user_operator.state.data}')
#     print(f'Stock operator state: {stock_operator.state.data}')
#     print(f'Order operator state: {order_operator.state.data}')
