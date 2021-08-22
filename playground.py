# from timeit import default_timer as timer
#
#
# def convert(in_str):
#     result = []
#     current_tuple = []
#     for token in in_str.split(","):
#         number = int(token.replace("(", "").replace(")", ""))
#         current_tuple.append(number)
#         if ")" in token:
#            result.append(tuple(current_tuple))
#            current_tuple = []
#     return result
#
#
# peers = [("worker-0", 8888), ("worker-1", 8888), ("worker-2", 8888), ("worker-3", 8888)]*1000
#
# str_peers = str(peers)
#
# print(type(str_peers))
#
#
# start = timer()
# eval_peers = eval(str_peers)
# end = timer()
# print("Eval took: %.3f ms" % (1000 * (end - start)))
#
# print(type(eval_peers))
#
#
# start = timer()
# eval_peers = convert(str_peers)
# end = timer()
# print("Convert took: %.3f ms" % (1000 * (end - start)))


# import concurrent.futures as cf
# import logging
# import time
#
# logger_format = '%(asctime)s:%(threadName)s:%(message)s'
# logging.basicConfig(format=logger_format, level=logging.INFO, datefmt="%H:%M:%S")
#
# num_word_mapping = {1: 'ONE', 2: 'TWO', 3: "THREE", 4: "FOUR", 5: "FIVE", 6: "SIX", 7: "SEVEN", 8: "EIGHT",
#                     9: "NINE", 10: "TEN"}
#
#
# def delay_message(delay, message):
#     logging.info(f"{message} received")
#     time.sleep(delay)
#     logging.info(f"Printing {message}")
#     return message
#
#
# if __name__ == '__main__':
#     with cf.ThreadPoolExecutor(max_workers=2) as executor:
#         future_to_mapping = {executor.submit(delay_message, i, num_word_mapping[i]): num_word_mapping[i] for i in
#                              range(2, 4)}
#         for future in cf.as_completed(future_to_mapping):
#             logging.info(f"{future.result()} Done")


# def print_parameters(*params):
#     print(params)
#
#
# if __name__ == '__main__':
#     print_parameters(1, 2, 3)

# class B(object):
#     def __call__(self):
#         return self.call_method(self)
#
#     @staticmethod
#     def call_method(self):
#         return 1
#
#
# def new_call(self):
#     return 42
#
#
# # Create two instances for testing.
# b1 = B()
# b2 = B()
# b2.call_method = new_call  # Will only affect this instance.
#
# print(b1())  # -> 1
# print(b2())  # -> 42

# n1 = 'name1'
# n2 = 'name2'
#
# d = {}
#
# d[frozenset({n1, n2})] = 'test'
#
# print(d)
#
# print(frozenset({n1, n2}) == frozenset({n2, n1}))
# print((n1, n2) == (n2, n1))


# from abc import ABC, abstractmethod
# from timeit import default_timer as timer
# from universalis_operator.opeartor import StateNotAttachedError
# from universalis_operator.state import OperatorState
#
#
# class StatefulFunction(ABC):
#
#     state: OperatorState
#
#     def __init__(self):
#         self.state = OperatorState()
#         self.name = type(self).__name__
#
#     def __call__(self, *args, **kwargs):
#         if self.state is None:
#             raise StateNotAttachedError('Cannot call stateful function without attached state')
#         return self.run(self, args)
#
#     def attach_state(self, operator_state: OperatorState):
#         self.state = operator_state
#
#     @abstractmethod
#     def run(self, *args):
#         raise NotImplementedError
#
#
# class CreateOrder(StatefulFunction):
#     def run(self, key, user_key):
#         self.state.create(key, {'user_key': user_key, 'items': []})
#         return key
#
#
# t1 = timer()
# co = CreateOrder()
# t2 = timer()
# co(1, 'asterios')
# t3 = timer()
# print(f'Instantiation took {(t2 - t1) * 1e6} us')
# print(f'Running took {(t3 - t2) * 1e6} us')
# print(co.name)
# from abc import ABC
#
#
# class A(ABC):
#
#     def __init__(self):
#         self.name = type(self).__name__
#
#
# class B(A):
#     pass
#
#
# def check(stuff):
#     for base in stuff.__class__.__bases__:
#         print(base.__name__)
#
#     print(A in type(stuff).__bases__)
#
#
# b = B()
#
# check(b)
#
# print(b.name)

workers = [1, 2, 3]

print(workers)

current_worker = workers.pop(0)
print('Current worker', current_worker)
print(workers)

workers.append(current_worker)
print(workers)
