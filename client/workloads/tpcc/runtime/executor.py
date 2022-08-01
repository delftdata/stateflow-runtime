# -----------------------------------------------------------------------
# Adapted from MongoDB-labs py-tpcc package:
# https://github.com/mongodb-labs/py-tpcc/blob/master/pytpcc/runtime/executor.py
# -----------------------------------------------------------------------
import asyncio
import time
from datetime import datetime
from typing import Type

from universalis.common.operator import Operator
from universalis.universalis import Universalis

from common.logging import logging
from workloads.tpcc.functions import customer, district
from workloads.tpcc.functions.graph import customer_operator, district_operator
from workloads.tpcc.util import rand, constants
from workloads.tpcc.util.benchmark_parameters import BenchmarkParameters
from workloads.tpcc.util.key import tuple_to_composite
from workloads.tpcc.util.scale_parameters import ScaleParameters


class Executor:
    def __init__(
            self,
            benchmark_parameters: BenchmarkParameters,
            scale_parameters: ScaleParameters,
            universalis: Universalis
    ):
        self.benchmark_parameters: BenchmarkParameters = benchmark_parameters
        self.scale_parameters: ScaleParameters = scale_parameters
        self.universalis: Universalis = universalis

    async def execute_transactions(self):
        tasks = []
        start = time.time()
        responses = []
        fun_cnts: dict = {district.NewOrder: 0, customer.Payment: 0}

        while (time.time() - start) <= self.benchmark_parameters.benchmark_duration:
            choice = rand.number(1, 100)

            if choice <= 49:
                operator, key, fun, params = self.generate_payment_params()
                fun_cnts[fun] += 1
            else:
                operator, key, fun, params = self.generate_new_order_params()
                fun_cnts[fun] += 1

            tasks.append(self.universalis.send_kafka_event(operator, key, fun, (key, params,)))

            if len(tasks) == self.benchmark_parameters.executor_batch_size:
                responses += await asyncio.gather(*tasks)
                tasks = []
                await asyncio.sleep(self.benchmark_parameters.executor_batch_wait_time)

        if len(tasks) > 0:
            responses += await asyncio.gather(*tasks)

        logging.info(fun_cnts)
        return responses

    def generate_new_order_params(self) -> tuple[Operator, str, Type, dict]:
        """Return parameters for NEW_ORDER"""
        params = {
            'w_id': self.make_warehouse_id(),
            'd_id': self.make_district_id(),
            'c_id': self.make_customer_id(),
            'o_entry_d': datetime.now().strftime("%m/%d/%Y, %H:%M:%S"),
        }
        key: str = tuple_to_composite((params['w_id'], params['d_id']))

        # 1% of transactions roll back
        rollback = rand.number(1, 100) == 1

        params['i_ids'] = []
        params['i_w_ids'] = []
        params['i_qtys'] = []
        ol_cnt = rand.number(constants.MIN_OL_CNT, constants.MAX_OL_CNT)

        for i in range(0, ol_cnt):
            # if rollback and i + 1 == ol_cnt:
            #     i_ids.append(self.scale_parameters.items + 1)
            # else:
            i_id = self.make_item_id()
            while i_id in params['i_ids']:
                i_id = self.make_item_id()
            params['i_ids'].append(i_id)

            # 1% of items are from a remote warehouse
            remote = (rand.number(1, 100) == 1)
            if self.scale_parameters.warehouses > 1 and remote:
                params['i_w_ids'].append(
                    rand.number_excluding(
                        self.scale_parameters.starting_warehouse,
                        self.scale_parameters.ending_warehouse,
                        params['w_id']
                    )
                )
            else:
                params['i_w_ids'].append(params['w_id'])

            params['i_qtys'].append(rand.number(1, constants.MAX_OL_QUANTITY))

        return district_operator, key, district.NewOrder, params

    def generate_payment_params(self) -> tuple[Operator, str, Type, dict]:
        """Return parameters for PAYMENT"""
        x = rand.number(1, 100)

        params = {
            'w_id': self.make_warehouse_id(),
            'd_id': self.make_district_id(),
            'c_id': self.make_customer_id(),
            'c_last': None,
            'h_amount': rand.fixed_point(2, constants.MIN_PAYMENT, constants.MAX_PAYMENT),
            'h_date': datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        }
        key: str = tuple_to_composite((params['w_id'], params['d_id'], params['c_id']))

        # 85%: paying through own warehouse (or there is only 1 warehouse)
        if self.scale_parameters.warehouses == 1 or x <= 85:
            params['c_w_id'] = params['w_id']
            params['c_d_id'] = params['d_id']
        # 15%: paying through another warehouse:
        else:
            # select in range [1, num_warehouses] excluding w_id
            params['c_w_id'] = rand.number_excluding(
                self.scale_parameters.starting_warehouse,
                self.scale_parameters.ending_warehouse,
                params['w_id']
            )
            assert params['c_w_id'] != params['w_id'], "Failed to generate W_ID that's not equal to C_W_ID"
            params['c_d_id'] = self.make_district_id()

        return customer_operator, key, customer.Payment, params

    def make_warehouse_id(self) -> int:
        w_id = rand.number(self.scale_parameters.starting_warehouse, self.scale_parameters.ending_warehouse)
        assert (w_id >= self.scale_parameters.starting_warehouse), "Invalid W_ID: %d" % w_id
        assert (w_id <= self.scale_parameters.ending_warehouse), "Invalid W_ID: %d" % w_id
        return w_id

    def make_district_id(self) -> int:
        return rand.number(1, self.scale_parameters.districts_per_warehouse)

    def make_customer_id(self) -> int:
        return rand.nu_rand(1023, 1, self.scale_parameters.customers_per_district)

    def make_item_id(self) -> int:
        return rand.nu_rand(8191, 1, self.scale_parameters.items)