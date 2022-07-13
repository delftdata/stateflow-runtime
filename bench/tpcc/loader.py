import logging
import asyncio

from datetime import datetime

from tpcc.functions.graph import item_operator, g
from tpcc.functions import item
from tpcc.util import rand, constants
from tpcc.util.scale_parameters import ScaleParameters
from universalis.universalis import Universalis


class Loader:
    def __init__(self, scale_parameters: ScaleParameters, w_ids: list[int], universalis: Universalis, batch_size: int = 10000):
        self.scale_parameters = scale_parameters
        self.w_ids: w_ids
        self.batch_size: int = batch_size
        self.universalis: Universalis = universalis

    async def execute(self):
        await self.load_items()

        # for w_id in self.w_ids:
        #     self.load_warehouse(w_id)

    async def load_items(self):
        # Select 10% of the rows to be marked "original"
        original_rows = rand.select_unique_ids(self.scale_parameters.items / 10, 1, self.scale_parameters.items)

        # Load all of the items
        tasks = []
        total_tuples = 0

        for i_id in range(1, self.scale_parameters.items+1):
            original = (i_id in original_rows)

            tasks.append(self.universalis.send_kafka_event(
                item_operator,
                i_id,
                item.InitialiseItems,
                (i_id, self.generate_item(i_id, original)))
            )
            total_tuples += 1

            if len(tasks) == self.batch_size:
                await asyncio.gather(*tasks)
                tasks = []
                print('yo')

                logging.info(
                    "LOAD - %s: %5d / %d" % (constants.TABLENAME_ITEM, total_tuples, self.scale_parameters.items)
                )

        if len(tasks) > 0:
            await asyncio.gather(*tasks)
            logging.debug("LOAD - %s: %5d / %d" % (constants.TABLENAME_ITEM, total_tuples, self.scale_parameters.items))

    def generate_item(self, i_id: int, original: bool) -> tuple[int, int, str, float, str]:
        i_im_id: int = rand.number(constants.MIN_IM, constants.MAX_IM)
        i_name: str = rand.a_string(constants.MIN_I_NAME, constants.MAX_I_NAME)
        i_price: float = rand.fixed_point(constants.MONEY_DECIMALS, constants.MIN_PRICE, constants.MAX_PRICE)
        i_data: str = rand.a_string(constants.MIN_I_DATA, constants.MAX_I_DATA)

        if original:
            i_data = self.fill_original(i_data)

        return i_id, i_im_id, i_name, i_price, i_data

    def generate_warehouse(self, w_id: int) -> tuple[int, str, str, str, str, str, str, float, float]:
        w_tax: float = self.generate_tax()
        w_ytd: float = constants.INITIAL_W_YTD
        w_address: tuple[str, str, str, str, str, str] = self.generate_address()
        return (w_id,) + w_address + (w_tax, w_ytd)

    def generate_district(self, d_w_id: int, d_id: int, d_next_o_id: int) \
            -> tuple[int, int, str, str, str, str, str, str, float, float, int]:
        d_tax: float = self.generate_tax()
        d_ytd: float = constants.INITIAL_D_YTD
        d_address: tuple[str, str, str, str, str, str] = self.generate_address()
        return (d_id, d_w_id,) + d_address + (d_tax, d_ytd, d_next_o_id)

    def generate_customer(self, c_w_id: int, c_d_id: int, c_id: int, bad_credit: bool) \
            -> tuple[
                int, int, int, str, str, str, str, str, str, str, str, str, datetime, str, float, float, float, float,
                int, int, str]:
        c_first: str = rand.a_string(constants.MIN_FIRST, constants.MAX_FIRST)
        c_middle: str = constants.MIDDLE

        assert 1 <= c_id <= constants.CUSTOMERS_PER_DISTRICT
        if c_id <= 1000:
            c_last: str = rand.make_last_name(c_id - 1)
        else:
            c_last: str = rand.make_last_name(constants.CUSTOMERS_PER_DISTRICT)

        c_phone: str = rand.n_string(constants.PHONE, constants.PHONE)
        c_since: datetime = datetime.now()
        c_credit: str = constants.BAD_CREDIT if bad_credit else constants.GOOD_CREDIT
        c_credit_lim: float = constants.INITIAL_CREDIT_LIM
        c_discount: float = rand.fixed_point(
            constants.DISCOUNT_DECIMALS,
            constants.MIN_DISCOUNT,
            constants.MAX_DISCOUNT
            )
        c_balance: float = constants.INITIAL_BALANCE
        c_ytd_payment: float = constants.INITIAL_YTD_PAYMENT
        c_payment_cnt: int = constants.INITIAL_PAYMENT_CNT
        c_delivery_cnt: int = constants.INITIAL_DELIVERY_CNT
        c_data: str = rand.a_string(constants.MIN_C_DATA, constants.MAX_C_DATA)

        c_street1: str = rand.a_string(constants.MIN_STREET, constants.MAX_STREET)
        c_street2: str = rand.a_string(constants.MIN_STREET, constants.MAX_STREET)
        c_city: str = rand.a_string(constants.MIN_CITY, constants.MAX_CITY)
        c_state: str = rand.a_string(constants.STATE, constants.STATE)
        c_zip: str = self.generate_zip()

        return c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street1, c_street2, c_city, c_state, c_zip, c_phone, \
            c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data

    def generate_order_line(
            self,
            ol_w_id: int,
            ol_d_id: int,
            ol_o_id: int,
            ol_number: int,
            max_items: int,
            new_order: bool
            ) -> \
            tuple[int, int, int, int, int, int, datetime | None, int, float, str]:
        ol_i_id: int = rand.number(1, max_items)
        ol_supply_w_id: int = ol_w_id
        ol_delivery_d: datetime | None = datetime.now()
        ol_quantity: int = constants.INITIAL_QUANTITY

        # 1% of items are from a remote warehouse
        remote: bool = (rand.number(1, 100) == 1)

        if self.scale_parameters.warehouses > 1 and remote:
            ol_supply_w_id: int = rand.number_excluding(
                self.scale_parameters.starting_warehouse,
                self.scale_parameters.ending_warehouse,
                ol_w_id
            )

        ol_amount: float = rand.fixed_point(
            constants.MONEY_DECIMALS,
            constants.MIN_AMOUNT,
            constants.MAX_PRICE * constants.MAX_OL_QUANTITY
        )

        if new_order:
            ol_delivery_d = None

        ol_dist_info: str = rand.a_string(constants.DIST, constants.DIST)

        return ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, \
               ol_dist_info

    def generate_stock(self, s_w_id: int, s_i_id: int, original: bool) -> tuple[int, int, int, list[str], int, int, int, str]:
        s_quantity: int = rand.number(constants.MIN_QUANTITY, constants.MAX_QUANTITY)
        s_ytd: int = 0
        s_order_cnt: int = 0
        s_remote_cnt: int = 0

        s_data: str = rand.a_string(constants.MIN_I_DATA, constants.MAX_I_DATA)
        if original:
            self.fill_original(s_data)

        s_dists: list[str] = []
        for i in range(0, constants.DISTRICTS_PER_WAREHOUSE):
            s_dists.append(rand.a_string(constants.DIST, constants.DIST))

        return s_i_id, s_w_id, s_quantity, s_dists, s_ytd, s_order_cnt, s_remote_cnt, s_data

    def generate_address(self,) -> tuple[str, str, str, str, str, str]:
        """
            Returns a name and a street address
            Used by both generate_warehouse() and generate_district().
        """
        name: str = rand.a_string(constants.MIN_NAME, constants.MAX_NAME)
        return (name,) + self.generate_street_address()

    def generate_street_address(self) -> tuple[str, str, str, str, str]:
        """
            Returns a list for a street address
            Used for warehouses, districts and customers.
        """
        street1: str = rand.a_string(constants.MIN_STREET, constants.MAX_STREET)
        street2: str = rand.a_string(constants.MIN_STREET, constants.MAX_STREET)
        city: str = rand.a_string(constants.MIN_CITY, constants.MAX_CITY)
        state: str = rand.a_string(constants.STATE, constants.STATE)
        g_zip: str = self.generate_zip()

        return street1, street2, city, state, g_zip

    @staticmethod
    def generate_history(h_c_w_id: int, h_c_d_id: int, h_c_id: int) \
            -> tuple[int, int, int, int, int, datetime, float, str]:
        h_w_id: int = h_c_w_id
        h_d_id: int = h_c_d_id
        h_date: datetime = datetime.now()
        h_amount: float = constants.INITIAL_AMOUNT
        h_data: str = rand.a_string(constants.MIN_DATA, constants.MAX_DATA)
        return h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data

    @staticmethod
    def generate_order(o_w_id: int, o_d_id: int, o_id: int, o_c_id: int, o_ol_cnt: int, new_order: bool) \
            -> tuple[int, int, int, int, datetime, int, int, int]:
        """Returns the generated o_ol_cnt value."""
        o_entry_d: datetime = datetime.now()
        o_carrier_id: int = constants.NULL_CARRIER_ID if new_order else rand.number(
            constants.MIN_CARRIER_ID,
            constants.MAX_CARRIER_ID
        )
        o_all_local: int = constants.INITIAL_ALL_LOCAL
        return o_id, o_c_id, o_d_id, o_w_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local

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
