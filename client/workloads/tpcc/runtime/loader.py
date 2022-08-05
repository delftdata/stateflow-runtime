# -----------------------------------------------------------------------
# Adapted from MongoDB-labs py-tpcc package:
# https://github.com/mongodb-labs/py-tpcc/blob/master/pytpcc/runtime/loader.py
# -----------------------------------------------------------------------

import asyncio
import logging
import random
from datetime import datetime

from universalis.universalis import Universalis
from workloads.tpcc.functions import item, warehouse, customer, history, order, order_line, new_order, district, stock
from workloads.tpcc.functions.graph import (
    item_operator,
    warehouse_operator,
    customer_operator,
    history_operator,
    order_operator, order_line_operator, new_order_operator, district_operator, stock_operator,
)
from workloads.tpcc.util import rand, constants
from workloads.tpcc.util.key import tuple_to_composite
from workloads.tpcc.util.scale_parameters import ScaleParameters


class Loader:
    def __init__(
            self,
            benchmark_parameters: dict,
            scale_parameters: ScaleParameters,
            w_ids: list[int],
            universalis: Universalis,
    ):
        self.loader_batch_size = benchmark_parameters['loader_batch_size']
        self.loader_batch_wait_time = benchmark_parameters['loader_batch_wait_time']
        self.scale_parameters = scale_parameters
        self.w_ids: w_ids = w_ids
        self.universalis: Universalis = universalis

    async def execute(self, run_number: int):
        requests = []
        requests += await self.load_items(run_number)
        await asyncio.sleep(self.loader_batch_wait_time)

        for w_id in self.w_ids:
            requests += await self.load_warehouse(w_id, run_number)

        return requests

    async def load_items(self, run_number: int):
        # Select 10% of the rows to be marked "original"
        original_rows = rand.select_unique_ids(
            round(self.scale_parameters.items / 10),
            1,
            self.scale_parameters.items
        )

        # Load all of the items
        tasks = []
        total_tuples = 0
        async_request_responses = []
        requests = []
        requests_meta: dict[int, (str, tuple)] = {}
        req_cnt: int = 0

        for i_id in range(1, self.scale_parameters.items + 1):
            original = (i_id in original_rows)

            key, params = self.generate_item(i_id, original)
            tasks.append(
                self.universalis.send_kafka_event(
                    item_operator,
                    key,
                    item.InsertItem,
                    (key, params)
                )
            )
            total_tuples += 1

            requests_meta[req_cnt] = ('InsertItem', (key, params))
            req_cnt += 1

            if len(tasks) == self.loader_batch_size:
                async_request_responses += await asyncio.gather(*tasks)
                tasks = []
                await asyncio.sleep(self.loader_batch_wait_time)
                logging.info(
                    "LOAD - %s: %5d / %d" % (constants.TABLENAME_ITEM, total_tuples, self.scale_parameters.items)
                )

        if len(tasks) > 0:
            async_request_responses += await asyncio.gather(*tasks)
            logging.info("LOAD - %s: %5d / %d" % (constants.TABLENAME_ITEM, total_tuples, self.scale_parameters.items))

        for i, request in enumerate(async_request_responses):
            request_id, timestamp = request
            requests += [{
                'run_number': run_number,
                'request_id': request_id,
                'function': requests_meta[i][0],
                'params': requests_meta[i][1],
                'stage': 'insertion',
                'timestamp': timestamp
            }]

        return requests

    async def load_warehouse(self, w_id: int, run_number: int):
        logging.info("LOAD - %s: %d / %d" % (constants.TABLENAME_WAREHOUSE, w_id, len(self.w_ids)))
        async_request_responses = []
        requests = []
        requests_meta: dict[int, (str, tuple)] = {}
        req_cnt: int = 0

        # WAREHOUSE
        key, params = self.generate_warehouse(w_id)
        async_request_responses += [await self.universalis.send_kafka_event(
            warehouse_operator,
            key,
            warehouse.InsertWarehouse,
            (key, params)
        )]

        requests_meta[req_cnt] = ('InsertWarehouse', (key, params))
        req_cnt += 1

        # DISTRICT
        for d_id in range(1, self.scale_parameters.districts_per_warehouse + 1):
            d_next_o_id = self.scale_parameters.customers_per_district + 1

            c_tasks = []
            h_tasks = []

            # Select 10% of the customers to have bad credit
            selected_rows = rand.select_unique_ids(
                round(self.scale_parameters.customers_per_district / 10),
                1,
                self.scale_parameters.customers_per_district
            )

            # TPC-C 4.3.3.1. says that o_c_id should be a permutation of [1, 3000]. But since it
            # is a c_id field, it seems to make sense to have it be a permutation of the
            # customers. For the "real" thing this will be equivalent
            c_id_permutation = []

            for c_id in range(1, self.scale_parameters.customers_per_district + 1):
                bad_credit = c_id in selected_rows

                key, params = self.generate_customer(w_id, d_id, c_id, bad_credit)
                c_tasks.append(
                    self.universalis.send_kafka_event(
                        customer_operator,
                        key,
                        customer.InsertCustomer,
                        (key, params)
                    )
                )

                requests_meta[req_cnt] = ('InsertCustomer', (key, params))
                req_cnt += 1

                if len(c_tasks) == self.loader_batch_size:
                    async_request_responses += await asyncio.gather(*c_tasks)
                    await asyncio.sleep(self.loader_batch_wait_time)
                    c_tasks = []

                key, params = self.generate_history(w_id, d_id, c_id)
                h_tasks.append(
                    self.universalis.send_kafka_event(
                        history_operator,
                        key,
                        history.InsertHistory,
                        (key, params)
                    )
                )

                requests_meta[req_cnt] = ('InsertHistory', (key, params))
                req_cnt += 1

                if len(h_tasks) == self.loader_batch_size:
                    async_request_responses += await asyncio.gather(*h_tasks)
                    await asyncio.sleep(self.loader_batch_wait_time)
                    h_tasks = []

                c_id_permutation.append(c_id)

            assert c_id_permutation[0] == 1
            assert c_id_permutation[self.scale_parameters.customers_per_district - 1] == \
                   self.scale_parameters.customers_per_district

            random.shuffle(c_id_permutation)

            o_tasks = []
            ol_tasks = []
            no_tasks = []

            for o_id in range(1, self.scale_parameters.customers_per_district + 1):
                o_ol_cnt = rand.number(constants.MIN_OL_CNT, constants.MAX_OL_CNT)

                # The last new_orders_per_district are new orders
                is_new_order = (
                        (self.scale_parameters.customers_per_district - self.scale_parameters.new_orders_per_district)
                        < o_id
                )

                key, params = self.generate_order(w_id, d_id, o_id, c_id_permutation[o_id - 1], o_ol_cnt, is_new_order)
                o_tasks.append(
                    self.universalis.send_kafka_event(
                        order_operator,
                        key,
                        order.InsertOrder,
                        (key, params)
                    )
                )

                requests_meta[req_cnt] = ('InsertOrder', (key, params))
                req_cnt += 1

                if len(o_tasks) > self.loader_batch_size:
                    async_request_responses += await asyncio.gather(*o_tasks)
                    await asyncio.sleep(self.loader_batch_wait_time)
                    o_tasks = []

                # Generate each order_line for the order
                for ol_number in range(0, o_ol_cnt):
                    key, params = self.generate_order_line(
                        w_id,
                        d_id,
                        o_id,
                        ol_number,
                        self.scale_parameters.items,
                        is_new_order
                    )

                    ol_tasks.append(
                        self.universalis.send_kafka_event(
                            order_line_operator,
                            key,
                            order_line.InsertOrderLine,
                            (key, params)
                        )
                    )

                    requests_meta[req_cnt] = ('InsertOrderLine', (key, params))
                    req_cnt += 1

                    if len(ol_tasks) > self.loader_batch_size:
                        async_request_responses += await asyncio.gather(*ol_tasks)
                        await asyncio.sleep(self.loader_batch_wait_time)
                        ol_tasks = []

                # This is a new order: make one for it
                if is_new_order:
                    no_key: str = tuple_to_composite((w_id, d_id, o_id))
                    no_tasks.append(
                        self.universalis.send_kafka_event(
                            new_order_operator,
                            no_key,
                            new_order.InsertNewOrder,
                            (no_key, (w_id, d_id, o_id))
                        )
                    )

                    requests_meta[req_cnt] = ('InsertNewOrder', (key, params))
                    req_cnt += 1

                    if len(no_tasks) > self.loader_batch_size:
                        async_request_responses += await asyncio.gather(*no_tasks)
                        await asyncio.sleep(self.loader_batch_wait_time)
                        no_tasks = []

            key, params = self.generate_district(w_id, d_id, d_next_o_id)

            async_request_responses += [await self.universalis.send_kafka_event(
                district_operator,
                key,
                district.InsertDistrict,
                (key, params)
            )]

            requests_meta[req_cnt] = ('InsertDistrict', (key, params))
            req_cnt += 1

            if len(c_tasks) > 0:
                async_request_responses += await asyncio.gather(*c_tasks)
                await asyncio.sleep(self.loader_batch_wait_time)
            if len(h_tasks) > 0:
                async_request_responses += await asyncio.gather(*h_tasks)
                await asyncio.sleep(self.loader_batch_wait_time)
            if len(o_tasks) > 0:
                async_request_responses += await asyncio.gather(*o_tasks)
                await asyncio.sleep(self.loader_batch_wait_time)
            if len(ol_tasks) > 0:
                async_request_responses += await asyncio.gather(*ol_tasks)
                await asyncio.sleep(self.loader_batch_wait_time)
            if len(no_tasks) > 0:
                async_request_responses += await asyncio.gather(*no_tasks)
                await asyncio.sleep(self.loader_batch_wait_time)

            logging.info(
                "LOAD - %s: %d / %d" % (
                    constants.TABLENAME_DISTRICT, d_id, self.scale_parameters.districts_per_warehouse)
            )

        # Select 10% of the stock to be marked "original"
        s_tasks = []
        selected_rows = rand.select_unique_ids(round(self.scale_parameters.items / 10), 1, self.scale_parameters.items)
        total_tuples = 0

        for i_id in range(1, self.scale_parameters.items + 1):
            original = (i_id in selected_rows)

            key, params = self.generate_stock(w_id, i_id, original)
            s_tasks.append(
                self.universalis.send_kafka_event(
                    stock_operator,
                    key,
                    stock.InsertStock,
                    (key, params)
                )
            )
            total_tuples += 1
            requests_meta[req_cnt] = ('InsertStock', (key, params))
            req_cnt += 1

            if len(s_tasks) == self.loader_batch_size:
                logging.info(
                    "LOAD - %s [W_ID=%d]: %5d / %d" % (
                        constants.TABLENAME_STOCK, w_id, total_tuples, self.scale_parameters.items)
                )

                async_request_responses += await asyncio.gather(*s_tasks)
                s_tasks = []

        if len(s_tasks) > 0:
            logging.info(
                "LOAD - %s [W_ID=%d]: %5d / %d" % (
                    constants.TABLENAME_STOCK, w_id, total_tuples, self.scale_parameters.items)
            )
            async_request_responses += await asyncio.gather(*s_tasks)

        for i, request in enumerate(async_request_responses):
            request_id, timestamp = request
            requests += [{
                'run_number': run_number,
                'request_id': request_id,
                'function': requests_meta[i][0],
                'params': requests_meta[i][1],
                'stage': 'insertion',
                'timestamp': timestamp
            }]

        return requests

    def generate_item(self, i_id: int, original: bool) -> tuple[str, dict[str, int | float | str]]:
        key: str = str(i_id)

        item_data: dict[str, int | float | str] = {
            'i_im_id': rand.number(constants.MIN_IM, constants.MAX_IM),
            'i_name': rand.a_string(constants.MIN_I_NAME, constants.MAX_I_NAME),
            'i_price': rand.fixed_point(constants.MONEY_DECIMALS, constants.MIN_PRICE, constants.MAX_PRICE),
            'i_data': rand.a_string(constants.MIN_I_DATA, constants.MAX_I_DATA)
        }

        if original:
            item_data['i_data'] = self.fill_original(item_data['i_data'])

        return key, item_data

    def generate_warehouse(self, w_id: int) -> tuple[str, dict[str, int | float | str]]:
        key: str = str(w_id)

        return key, {
            'w_tax': self.generate_tax(),
            'w_ytd': constants.INITIAL_W_YTD,
        } | self.generate_address('w_')

    def generate_district(self, d_w_id: int, d_id: int, d_next_o_id: int) -> tuple[str, dict[str, int | float | str]]:
        key: str = tuple_to_composite((d_w_id, d_id))

        return key, {
            'd_tax': self.generate_tax(),
            'd_ytd': constants.INITIAL_D_YTD,
            'd_next_o_id': d_next_o_id
        } | self.generate_address('d_')

    def generate_customer(self, c_w_id: int, c_d_id: int, c_id: int, bad_credit: bool) \
            -> tuple[str, dict[str, int | float | str]]:
        key: str = tuple_to_composite((c_w_id, c_d_id, c_id))

        return key, {
            'c_first': rand.a_string(constants.MIN_FIRST, constants.MAX_FIRST),
            'c_middle': constants.MIDDLE,
            'c_last': rand.make_last_name(c_id - 1) if c_id < 1000
            else rand.make_random_last_name(constants.CUSTOMERS_PER_DISTRICT),
            'c_phone': rand.n_string(constants.PHONE, constants.PHONE),
            'c_since': datetime.now().strftime("%m/%d/%Y, %H:%M:%S"),
            'c_credit': constants.BAD_CREDIT if bad_credit else constants.GOOD_CREDIT,
            'c_credit_lim': constants.INITIAL_CREDIT_LIM,
            'c_discount': rand.fixed_point(
                constants.DISCOUNT_DECIMALS,
                constants.MIN_DISCOUNT,
                constants.MAX_DISCOUNT
            ),
            'c_balance': constants.INITIAL_BALANCE,
            'c_ytd_payment': constants.INITIAL_YTD_PAYMENT,
            'c_payment_cnt': constants.INITIAL_PAYMENT_CNT,
            'c_delivery_cnt': constants.INITIAL_DELIVERY_CNT,
            'c_data': rand.a_string(constants.MIN_C_DATA, constants.MAX_C_DATA),
        } | self.generate_street_address('c_')

    def generate_order_line(
            self,
            ol_w_id: int,
            ol_d_id: int,
            ol_o_id: int,
            ol_number: int,
            max_items: int,
            new_order: bool
    ) -> tuple[str, dict[str, str | None | int | float]]:
        key: str = tuple_to_composite((ol_w_id, ol_d_id, ol_o_id, ol_number))

        # 1% of items are from a remote warehouse
        remote: bool = (rand.number(1, 100) == 1)

        return key, {
            'ol_i_id': rand.number(1, max_items),
            'ol_supply_w_id': ol_w_id if self.scale_parameters.warehouses <= 1 or not remote else rand.number_excluding(
                self.scale_parameters.starting_warehouse,
                self.scale_parameters.ending_warehouse,
                ol_w_id
            ),
            'ol_delivery_d': datetime.now().strftime("%m/%d/%Y, %H:%M:%S") if not new_order else None,
            'ol_quantity': constants.INITIAL_QUANTITY,
            'ol_amount': rand.fixed_point(
                constants.MONEY_DECIMALS,
                constants.MIN_AMOUNT,
                constants.MAX_PRICE * constants.MAX_OL_QUANTITY
            ),
            'ol_dist_info': rand.a_string(constants.DIST, constants.DIST),
        }

    def generate_stock(self, s_w_id: int, s_i_id: int, original: bool) -> tuple[str, dict[str, str]]:
        key = tuple_to_composite((s_w_id, s_i_id))

        stock_params = {
            's_quantity': rand.number(constants.MIN_QUANTITY, constants.MAX_QUANTITY),
            's_ytd': 0,
            's_order_cnt': 0,
            's_remote_cnt': 0,
            's_data': self.fill_original(rand.a_string(constants.MIN_I_DATA, constants.MAX_I_DATA)) if original
            else rand.a_string(constants.MIN_I_DATA, constants.MAX_I_DATA),
        }

        for i in range(1, constants.DISTRICTS_PER_WAREHOUSE + 1):
            stock_params[f's_dist_{i}'] = rand.a_string(constants.DIST, constants.DIST)

        return key, stock_params

    def generate_address(self, prefix: str = '') -> dict[str, str]:
        """
            Returns a name and a street address
            Used by both generate_warehouse() and generate_district().
        """
        return {prefix + 'name': rand.a_string(constants.MIN_NAME, constants.MAX_NAME)} | \
               self.generate_street_address(prefix)

    def generate_street_address(self, prefix: str = '') -> dict[str, str]:
        """
            Returns a list for a street address
            Used for warehouses, districts and customers.
        """
        return {
            prefix + 'street_1': rand.a_string(constants.MIN_STREET, constants.MAX_STREET),
            prefix + 'street_2': rand.a_string(constants.MIN_STREET, constants.MAX_STREET),
            prefix + 'city': rand.a_string(constants.MIN_CITY, constants.MAX_CITY),
            prefix + 'state': rand.a_string(constants.STATE, constants.STATE),
            prefix + 'zip': self.generate_zip()
        }

    @staticmethod
    def generate_history(h_c_w_id: int, h_c_d_id: int, h_c_id: int) -> tuple[str, dict[str, int | str | float]]:
        key: str = tuple_to_composite((h_c_w_id, h_c_d_id, h_c_id))

        return key, {
            'h_d_id': h_c_d_id,
            'h_w_id': h_c_w_id,
            'h_date': datetime.now().strftime("%m/%d/%Y, %H:%M:%S"),
            'h_amount': constants.INITIAL_AMOUNT,
            'h_data': rand.a_string(constants.MIN_DATA, constants.MAX_DATA)
        }

    @staticmethod
    def generate_order(o_w_id: int, o_d_id: int, o_id: int, o_c_id: int, o_ol_cnt: int, is_new_order: bool) -> \
            tuple[str, dict[str, int | float | str]]:
        key: str = tuple_to_composite((o_w_id, o_d_id, o_id))

        return key, {
            'o_c_id': o_c_id,
            'o_entry_d': datetime.now().strftime("%m/%d/%Y, %H:%M:%S"),
            'o_carrier_id': constants.NULL_CARRIER_ID if is_new_order else rand.number(
                constants.MIN_CARRIER_ID,
                constants.MAX_CARRIER_ID
            ),
            'o_ol_cnt': o_ol_cnt,
            'o_all_local': constants.INITIAL_ALL_LOCAL,
        }

    @staticmethod
    def generate_tax() -> float:
        return rand.fixed_point(constants.TAX_DECIMALS, constants.MIN_TAX, constants.MAX_TAX)

    @staticmethod
    def generate_zip() -> str:
        length = constants.ZIP_LENGTH - len(constants.ZIP_SUFFIX)
        return rand.n_string(length, length) + constants.ZIP_SUFFIX

    @staticmethod
    def fill_original(data: str) -> str:
        """
            a string with ORIGINAL_STRING at a random position
        """
        original_length = len(constants.ORIGINAL_STRING)
        position = rand.number(0, len(data) - original_length)
        out = str(data[:position] + constants.ORIGINAL_STRING + data[position + original_length:])
        assert len(out) == len(data)

        return out
