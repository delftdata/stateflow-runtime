import asyncio

from universalis.common.logging import logging


class Sequencer:

    def __init__(self):
        self.distributed_log: list = []
        self.t_counter = 0
        self.worker_id = -1
        self.n_workers = -1
        self.prev_epoch_idx = 0
        self.epoch_counter = 0
        self.distributed_log_lock = asyncio.Lock()

    def set_worker_id(self, worker_id: int):
        self.worker_id = worker_id

    def set_n_workers(self, n_workers: int):
        self.n_workers = n_workers

    async def sequence(self, message: object):
        async with self.distributed_log_lock:
            t_id = self.worker_id + self.t_counter * self.n_workers
            self.distributed_log.append((t_id, message))
            self.t_counter += 1

    async def get_epoch(self) -> list[tuple[int, object]]:
        async with self.distributed_log_lock:
            epoch = self.distributed_log[self.prev_epoch_idx:self.t_counter]
            if len(epoch) > 0:
                logging.info(f"Sequencer Epoch: {self.epoch_counter}")
                self.prev_epoch_idx = self.t_counter
                self.epoch_counter += 1
                return epoch

    def cleanup(self):
        self.distributed_log: list = []
        self.t_counter = 0
        self.prev_epoch_idx = 0
        self.epoch_counter = 0
