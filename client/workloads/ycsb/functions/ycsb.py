from common.logging import logging
from universalis.common.stateful_function import StatefulFunction
from workloads.ycsb.util import consts


class NotEnoughCredit(Exception):
    pass


class Insert(StatefulFunction):
    async def run(self, key: str):
        value: int = consts.initial_account_balance
        await self.put(key, value)
        # return key
        # logging.warning(f'Remote write within transaction: {key} within INSERT')
        return key, value


class Read(StatefulFunction):
    async def run(self, key: str):
        value: int = await self.get(key)
        # logging.warning(f'Remote read within transaction: {data} within READ')
        return key, value


class Update(StatefulFunction):
    async def run(self, key: str):
        new_value = await self.get(key)
        new_value += consts.transfer_amount

        await self.put(key, new_value)
        return key, new_value


class Transfer(StatefulFunction):
    async def run(self, key_a: str, key_b: str):
        value_a = await self.get(key_a)

        self.call_remote_async(
            operator_name='ycsb',
            function_name=Update,
            key=key_b,
            params=(key_b,)
        )

        value_a -= consts.transfer_amount

        if value_a < 0:
            raise NotEnoughCredit(f'Not enough credit for user: {key_a}')

        await self.put(key_a, value_a)

        return key_a, value_a


class Debug(StatefulFunction):
    async def run(self, ins_key: int):
        logging.warning(f'START Remote read within transaction: {ins_key} within DEBUG')

        await self.call_remote_function_no_response(
            'ycsb',
            Insert,
            ins_key,
            (ins_key,)
        )

        read_own_remote_write = await self.call_remote_function_request_response(
            'ycsb',
            Read,
            ins_key,
            (ins_key,)
        )
        logging.warning(f'Remote read within transaction: {read_own_remote_write} within DEBUG')
        return read_own_remote_write
