# -----------------------------------------------------------------------
# Adapted from MongoDB-labs py-tpcc package:
# https://github.com/mongodb-labs/py-tpcc/blob/master/pytpcc/runtime/executor.py
# -----------------------------------------------------------------------
from datetime import datetime
from typing import Type

from universalis.common.operator import Operator
from universalis.universalis import Universalis

from workloads.tpcc.util import rand, constants
from workloads.tpcc.util.scale_parameters import ScaleParameters


class Executor:
    def __init__(self, scale_parameters: ScaleParameters, universalis: Universalis):
        self.scale_parameters: ScaleParameters = scale_parameters
        self.universalis: Universalis = universalis

    async def execute_transaction(self):
        # x = rand.number(1, 100)
        #
        # if x <= 50:
        #     operator, key, fun, params = self.generate_payment_params()
        # else:
        operator, key, fun, params = self.generate_new_order_params()
        await self.universalis.send_kafka_event(constants.OPERATOR_ORDER, key, fun, (params,))

    def generate_new_order_params(self) -> tuple[Operator, str, Type, dict]:
        """Return parameters for NEW_ORDER"""
        params = {
            'w_id': self.make_warehouse_id(),
            'd_id': self.make_district_id(),
            'o_entry_d': datetime.now().strftime("%m/%d/%Y, %H:%M:%S"),
        }
        # 1% of transactions roll back
        rollback = rand.number(1, 100) == 1

        i_ids = []
        i_w_ids = []
        i_qtys = []
        ol_cnt = rand.number(constants.MIN_OL_CNT, constants.MAX_OL_CNT)

        for i in range(0, ol_cnt):
            if rollback and i + 1 == ol_cnt:
                i_ids.append(self.scale_parameters.items + 1)
            else:
                i_id = self.make_item_id()
                while i_id in i_ids:
                    i_id = self.make_item_id()
                i_ids.append(i_id)

            # 1% of items are from a remote warehouse
            remote = (rand.number(1, 100) == 1)
            if self.scale_parameters.warehouses > 1 and remote:
                i_w_ids.append(
                    rand.number_excluding(
                        self.scale_parameters.starting_warehouse,
                        self.scale_parameters.ending_warehouse,
                        params['w_id']
                    )
                )
            else:
                i_w_ids.append(params['w_id'])

            i_qtys.append(rand.number(1, constants.MAX_OL_QUANTITY))

        params['i_ids'] = i_ids
        params['i_qtys'] = i_qtys
        params['i_w_ids'] = i_w_ids

        return constants.OPERATOR_CUSTOMER, str(self.make_customer_id()), constants.FUNCTIONS_ORDER.NewOrder, params

    def generate_payment_params(self) -> tuple[Operator, str, Type, dict]:
        """Return parameters for PAYMENT"""
        x = rand.number(1, 100)
        key: str = str(self.make_customer_id())

        params = {
            'w_id': self.make_warehouse_id(),
            'd_id': self.make_district_id(),
            'c_last': None,
            'h_amount': rand.fixed_point(2, constants.MIN_PAYMENT, constants.MAX_PAYMENT),
            'h_date': datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        }

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

        return constants.OPERATOR_CUSTOMER, key, constants.FUNCTIONS_CUSTOMER.Payment, params

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
