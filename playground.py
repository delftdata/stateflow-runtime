import copy
import fractions
import itertools
import math
import random
from typing import Awaitable

from aiokafka import AIOKafkaConsumer
import asyncio
import uvloop


# async def consume():
#     consumer = AIOKafkaConsumer(
#         'universalis-egress',
#         bootstrap_servers='localhost:9093')
#     await consumer.start()
#     try:
#         # Consume messages
#         async for msg in consumer:
#             print("consumed: ", msg.topic, msg.partition, msg.offset,
#                   msg.key, msg.value, msg.timestamp)
#     finally:
#         # Will leave consumer group; perform autocommit if enabled.
#         await consumer.stop()
#
#
# uvloop.install()
# asyncio.run(consume())
# class State:
#     pass
#
#
# class F:
#
#     state: State
#
#     def __init__(self, name: str):
#         self.name = name
#
#     def attach_state(self, state):
#         self.state = state
#
#
# class Operator:
#
#     def __init__(self):
#         self.state = State()
#         print(f"Operator's state {self.state}")
#         self.functions: dict[int, F] = {0: F('f1')}
#
#     def function_factory(self, idx, new_name: str) -> F:
#         new_function = copy.copy(self.functions[idx])
#         new_function.attach_state(self.state)
#         new_function.name = new_name
#         print(f"New function's state {new_function.state} with name: {new_function.name}")
#         return new_function
#
#
# op = Operator()
# print(op)
#
# obj1 = op.function_factory(0, 'f2')
# print(obj1)
# obj2 = op.function_factory(0, 'f3')
# print(obj2)
# print(obj1 == obj2)
# print(obj1.state == obj2.state)
# print(op.function_factory(0, 'f4'))
# print(op.function_factory(0, 'f5'))
# l = []
# l.append(1)
# print(l)
# l.append(2)
# print(l)
#
# list2d = [[1, 2, 3], [4, 5], [6]]
#
# merged = list(itertools.chain.from_iterable(list2d))
# print(merged)
# from timeit import default_timer as timer
#
#
# N = 100000
# idx1 = 1
# idx2 = 2
# idx3 = 3
# workers = 3
# seq1 = []
# seq2 = []
# seq3 = []
# for i in range(N):
#     seq1.append(idx1 + i * workers)
#     seq2.append(idx2 + i * workers)
#     seq3.append(idx3 + i * workers)
#
#
# # print(f'seq1: {seq1}')
# # print(f'seq2: {seq2}')
# # print(f'seq3: {seq3}')
#
#
# abr1 = random.sample(seq1, k=1000) + random.sample(seq2, k=600) + random.sample(seq3, k=500)
# set_seq1 = set(seq1)
# set_abr1 = set(abr1)
#
#
# def sequence_aborts_for_next_epoch1(seq: list, aborts: list):
#     to_append = []
#     for item in aborts:
#         if item in seq:
#             to_append.append(item)
#     return to_append
#
#
# def sequence_aborts_for_next_epoch2(seq: set, aborts: set):
#     return aborts.intersection(seq)
#
#
# start = timer()
# res1 = sequence_aborts_for_next_epoch1(seq1, abr1)
# end = timer()
# print(f'time: {end - start}')
#
# start = timer()
# res2 = sequence_aborts_for_next_epoch2(set_seq1, set_abr1)
# end = timer()
# print(f'time: {end - start}')
#
# s1 = sorted(res1)
# s2 = sorted(list(res2))
# correctness = s1 == s2
# print(correctness)
#
#
# start = timer()
# res3 = N in seq1
# end = timer()
# print(f'time: {end - start}')
#
# start = timer()
# res4 = N in set_seq1
# end = timer()
# print(f'time: {end - start}')

# def get_max_counter(my, remote):
#     return max(*remote, my)
#
#
# counters = {1: 1, 2: 3, 3: 4}
# my_c = 5
#
# print(get_max_counter(my_c, counters.values()))

#
# import asyncio, random
#
#
# async def rnd_sleep(t):
#     # sleep for T seconds on average
#     await asyncio.sleep(t * random.random() * 2)
#
#
# async def producer(queue):
#     while True:
#         # produce a token and send it to a consumer
#         token = random.random()
#         print(f'produced {token}')
#         if token < .05:
#             break
#         await queue.put(token)
#         await rnd_sleep(.1)
#
#
# async def consumer(queue):
#     while True:
#         token = await queue.get()
#         # process the token received from a producer
#         await rnd_sleep(.3)
#         queue.task_done()
#         print(f'consumed {token}')
#
#
# async def main():
#     queue = asyncio.Queue()
#
#     # fire up the both producers and consumers
#     producers = [asyncio.create_task(producer(queue))
#                  for _ in range(3)]
#     consumers = [asyncio.create_task(consumer(queue))
#                  for _ in range(10)]
#
#     # with both producers and consumers running, wait for
#     # the producers to finish
#     await asyncio.gather(*producers)
#     print('---- done producing')
#
#     # wait for the remaining tasks to be processed
#     await queue.join()
#     print('---- queue joined')
#
#     # cancel the consumers, which are now idle
#     for c in consumers:
#         c.cancel()


# asyncio.run(main())

from timeit import default_timer as timer


# N = 1000
#
# for i in range(N):
#     start = timer()
#     n = i+1
#     s = sum([fractions.Fraction(f'1/{n}')]*n)
#     if s != 1:
#         print(f'Error at n={n} with s={s}')
#     end = timer()
#     print(f'Calculating at n={n} with s={s} took: {end - start}')

# print(fractions.Fraction.from_float(l[0]))

# print(fractions.Fraction('1/9')/9)
# print(str(fractions.Fraction('1/9')*fractions.Fraction('1/9')))
# print(fractions.Fraction('2/9') + fractions.Fraction('2/9') + fractions.Fraction(0))
#
# start = timer()
# print(fractions.Fraction(0))
# end = timer()
# print(end - start)


# async def wait_and_print(timeout):
#     await asyncio.sleep(timeout)
#     print(f'executed: {timeout}')
#
#
# async def create_future():
#     tasks: list[Awaitable] = []
#     for i in range(10):
#         tasks.append(wait_and_print(i))
#     await asyncio.sleep(2)
#     print('Starting gather')
#     await asyncio.gather(*tasks)
#
#
# asyncio.run(create_future())
#
# N = 100000
#
# class A:
#
#     def __init__(self):
#         self.l = []
#
#
# a = A()
#
# cpl = []
# start = timer()
# for i in range(N):
#     cpl.append(copy.copy(a))
# end = timer()
# print(end-start)
#
# dpl = []
# start = timer()
# for i in range(N):
#     dpl.append(copy.deepcopy(a))
# end = timer()
# print(end-start)


# import uuid
# from timeit import default_timer as timer
#
# N = 1000000
# start = timer()
# for i in range(N):
#     # request_id = uuid.uuid4().int & (1 << 64)-1
#     request_id = uuid.uuid1().int >> 64
#     print(request_id)
# end = timer()
# print(f'{round((end - start) * 1000, 4)}ms')


# import traceback
# import sys
#
# d = {}
#
# try:
#     print(d['test'])
# except Exception:
#     print(traceback.format_exc())
#     # or
#     print(sys.exc_info()[2])


# class Base:
#     pass
#
#
# class A(Base):
#
#     def __init__(self):
#         self.text = 'a'
#
#
# class_def = A
#
# print(class_def.__name__)
#
# object_a = class_def()
#
# print(type(object_a))
#
# object_a_again = class_def()
#
# print(object_a_again)
#
# from demo.functions import user
#
#
# print(type(user.SubtractCredit).__name__)


# class Count:
#     """Iterator that counts upward forever."""
#
#     def __init__(self, start=0):
#         self.num = start
#
#     def __iter__(self):
#         return self
#
#     def __next__(self):
#         num = self.num
#         self.num += 1
#         return num
#
#
# c = Count()
#
# for i in range(10):
#     print(next(c))


from sortedcontainers import SortedList

N = 1000

s1 = set(range(0, N//2))
s2 = set(range(N//2, N))



# l =
# print(l)
#
# start = timer()
# s = set(l)
# end = timer()
# print(end - start)
# # print(s)
#
# sl = SortedList(range(N))
# print(sl)
#
# ssl = sl + sl  # intersection
# print(ssl)

# d = list(range(100))
# n = 50
# print(d)
# print(d[:n])
# print(d[n:])

# from dataclasses import dataclass
#
#
# @dataclass
# class SequencedItem:
#     t_id: int
#     msg: str
#
#     def __hash__(self):
#         return hash(self.t_id)
#
#     def __lt__(self, other):
#         # Lower t_id higher priority
#         return self.t_id < other.t_id
#
#
# N = 10
#
# items = [SequencedItem(i, str(i)) for i in range(N)]
#
# aborted = set(range(1, N))
#
# aborted_sequence_to_reschedule: set[SequencedItem] = {item for item in items if item.t_id in aborted}
# print(aborted_sequence_to_reschedule)

print("0" is None)
