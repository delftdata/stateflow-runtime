# -----------------------------------------------------------------------
# Adapted from MongoDB-labs py-tpcc package:
# https://github.com/mongodb-labs/py-tpcc/blob/master/pytpcc/runtime/executor.py
# -----------------------------------------------------------------------

from datetime import datetime

from workloads.tpcc.util import rand, constants
from workloads.tpcc.util.scale_parameters import ScaleParameters


class Executor:
    def __init__(self, scale_parameters: ScaleParameters):
        self.scale_parameters: ScaleParameters = scale_parameters

    def select_transaction(self):
        x = rand.number(1, 100)
        if x <= 50:
            txn, params = (constants.FUNCTIONS_ORDER.NewOrder, self.generate_payment_params())
        else:
            txn, params = (constants.FUNCTIONS_CUSTOMER.Payment, self.generate_new_order_params())

        return txn, params

    def generate_new_order_params(self):
        """Return parameters for NEW_ORDER"""
        w_id = self.make_warehouse_id()
        d_id = self.make_district_id()
        c_id = self.make_customer_id()
        ol_cnt = rand.number(constants.MIN_OL_CNT, constants.MAX_OL_CNT)
        o_entry_d = datetime.now()

        ## 1% of transactions roll back
        rollback = rand.number(1, 100) == 1

        i_ids = []
        i_w_ids = []
        i_qtys = []
        for i in range(0, ol_cnt):
            if rollback and i + 1 == ol_cnt:
                i_ids.append(self.scale_parameters.items + 1)
            else:
                i_id = self.make_item_id()
                while i_id in i_ids:
                    i_id = self.make_item_id()
                i_ids.append(i_id)

            ## 1% of items are from a remote warehouse
            remote = (rand.number(1, 100) == 1)
            if self.scale_parameters.warehouses > 1 and remote:
                i_w_ids.append(
                    rand.number_excluding(
                        self.scale_parameters.starting_warehouse,
                        self.scale_parameters.ending_warehouse,
                        w_id
                    )
                )
            else:
                i_w_ids.append(w_id)

            i_qtys.append(rand.number(1, constants.MAX_OL_QUANTITY))

        return w_id, d_id, c_id, o_entry_d, i_ids, i_w_ids, i_qtys

    def generate_payment_params(self):
        """Return parameters for PAYMENT"""
        x = rand.number(1, 100)
        y = rand.number(1, 100)

        w_id = self.make_warehouse_id()
        d_id = self.make_district_id()
        c_id = self.make_customer_id()
        c_last = None
        h_amount = rand.fixed_point(2, constants.MIN_PAYMENT, constants.MAX_PAYMENT)
        h_date = datetime.now()

        ## 85%: paying through own warehouse (or there is only 1 warehouse)
        if self.scale_parameters.warehouses == 1 or x <= 85:
            c_w_id = w_id
            c_d_id = d_id
        ## 15%: paying through another warehouse:
        else:
            ## select in range [1, num_warehouses] excluding w_id
            c_w_id = rand.number_excluding(
                self.scale_parameters.starting_warehouse,
                self.scale_parameters.ending_warehouse,
                w_id
            )
            assert c_w_id != w_id, "Failed to generate W_ID that's not equal to C_W_ID"
            c_d_id = self.make_district_id()

        return w_id, d_id, h_amount, c_w_id, c_d_id, c_id, c_last, h_date

    def make_warehouse_id(self):
        w_id = rand.number(self.scale_parameters.starting_warehouse, self.scale_parameters.ending_warehouse)
        assert (w_id >= self.scale_parameters.starting_warehouse), "Invalid W_ID: %d" % w_id
        assert (w_id <= self.scale_parameters.ending_warehouse), "Invalid W_ID: %d" % w_id
        return w_id

    def make_district_id(self):
        return rand.number(1, self.scale_parameters.districts_per_warehouse)

    def make_customer_id(self):
        return rand.NURandC(1023, 1, self.scale_parameters.customers_per_district)

    def make_item_id(self):
        return rand.NURandC(8191, 1, self.scale_parameters.items)
