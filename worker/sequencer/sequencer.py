import asyncio

from universalis.common.logging import logging

from worker.run_func_payload import RunFuncPayload, RunRemoteFuncPayload


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

    async def sequence(self, message: RunFuncPayload | RunRemoteFuncPayload):
        async with self.distributed_log_lock:
            if isinstance(message, RunRemoteFuncPayload):
                t_id = message.t_id
            else:
                t_id = self.worker_id + self.t_counter * self.n_workers
                self.t_counter += 1
            self.distributed_log.append((t_id, message))

    async def get_epoch(self) -> list[tuple[int, RunFuncPayload | RunRemoteFuncPayload]]:
        async with self.distributed_log_lock:
            # epoch = self.distributed_log[self.prev_epoch_idx:self.t_counter]
            if len(self.distributed_log) > 0:
                logging.info(f"Sequencer Epoch: {self.epoch_counter} with items: {self.distributed_log}")
                # self.prev_epoch_idx = self.t_counter
                self.epoch_counter += 1
                res = sorted(self.distributed_log, key=lambda f: f[0])
                self.distributed_log = []
                return res

    def cleanup(self):
        self.distributed_log: list = []
        self.t_counter = 0
        self.prev_epoch_idx = 0
        self.epoch_counter = 0
