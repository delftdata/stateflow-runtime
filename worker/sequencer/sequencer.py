import asyncio

from universalis.common.logging import logging

from worker.run_func_payload import RunFuncPayload, SequencedItem


class Sequencer:

    def __init__(self):
        self.distributed_log: set[SequencedItem] = set()
        self.t_counter = 0
        self.worker_id = -1
        self.n_workers = -1
        self.epoch_counter = 0
        self.distributed_log_lock = asyncio.Lock()

    def set_worker_id(self, worker_id: int):
        self.worker_id = worker_id

    def set_n_workers(self, n_workers: int):
        self.n_workers = n_workers

    async def sequence(self, message: RunFuncPayload):
        async with self.distributed_log_lock:
            t_id = self.worker_id + self.t_counter * self.n_workers
            self.t_counter += 1
            logging.info(f'Sequencing message: {message.key} with t_id: {t_id}')
            self.distributed_log.add(SequencedItem(t_id, message))

    async def sequence_aborts_for_next_epoch(self, aborted: set):
        async with self.distributed_log_lock:
            aborted_sequence_to_reschedule: set = self.distributed_log.intersection(aborted)
            self.distributed_log: set = self.distributed_log.union(aborted_sequence_to_reschedule)

    async def get_epoch(self) -> set[SequencedItem]:
        async with self.distributed_log_lock:
            if len(self.distributed_log) > 0:
                epoch = self.distributed_log
                self.distributed_log = set()
                return epoch

    async def increment_epoch(self, remote_t_counters):
        async with self.distributed_log_lock:
            self.epoch_counter += 1
            self.t_counter = max(*remote_t_counters, self.t_counter)
