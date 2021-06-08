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

class B(object):
    def __call__(self):
        return self.call_method(self)

    @staticmethod
    def call_method(self):
        return 1


def new_call(self):
    return 42


# Create two instances for testing.
b1 = B()
b2 = B()
b2.call_method = new_call  # Will only affect this instance.

print(b1())  # -> 1
print(b2())  # -> 42

