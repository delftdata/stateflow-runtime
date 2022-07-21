from universalis.common.stateful_function import StatefulFunction

from workloads.tpcc.util import constants
from workloads.tpcc.util.key import tuple_to_composite


class InvalidItemId(Exception):
    pass


class InsertCustomer(StatefulFunction):
    async def run(self, key: str, customer: dict[str, int | float | str]):
        await self.put(key, customer)
        return key, customer


class GetCustomer(StatefulFunction):
    async def run(self, key: str):
        customer = await self.get(key)
        return customer


class Payment(StatefulFunction):
    async def run(self, key: str, params: dict):
        # Initialize transaction properties
        w_id: str = params['w_id']
        d_id: str = params['d_id']
        h_amount: float = params['h_amount']
        c_w_id: str = params['c_w_id']
        c_d_id: str = params['c_d_id']
        c_last: str = params['c_last']
        h_date: str = params['h_date']

        # --------------------------
        # Get Customer By ID Query
        # --------------------------
        customer_data = await self.get(key)

        c_balance: float = float(customer_data['c_balance']) - h_amount
        c_ytd_payment: float = float(customer_data['c_ytd_payment']) + h_amount
        c_payment_cnt: float = float(customer_data['c_payment_cnt']) + 1
        c_data: str = customer_data['c_data']

        # ---------------------
        # Get Warehouse Query
        # ---------------------
        warehouse_key = str(w_id)
        warehouse_data: dict = await self.call_remote_function_request_response(
            'warehouse',
            'GetWarehouse',
            warehouse_key,
            (warehouse_key,)
        )

        # --------------------
        # Get District Query
        # --------------------
        district_key = tuple_to_composite((w_id, d_id))
        district_data: dict = await self.call_remote_function_request_response(
            'district',
            'GetDistrict',
            district_key,
            (district_key,)
        )

        # --------------------------------
        # Update Warehouse Balance Query
        # --------------------------------
        warehouse_data['w_ytd'] = float(warehouse_data['w_ytd']) + h_amount
        await self.call_remote_function_no_response(
            'warehouse',
            'InsertWarehouse',
            warehouse_key,
            (warehouse_key, warehouse_data)
        )

        # -------------------------------
        # Update District Balance Query
        # -------------------------------
        district_data['d_ytd'] = float(district_data['d_ytd']) + h_amount
        await self.call_remote_function_no_response(
            'district',
            'InsertDistrict',
            district_key,
            (district_key, district_data)
        )

        if customer_data['c_credit'] == constants.BAD_CREDIT:
            # ----------------------------------
            # Update Bad Credit Customer Query
            # ----------------------------------
            new_data = " ".join(map(str, [key, c_d_id, c_w_id, d_id, w_id, h_amount]))
            c_data = (new_data + "|" + c_data)

            if len(c_data) > constants.MAX_C_DATA:
                c_data = c_data[:constants.MAX_C_DATA]
        else:
            # -----------------------------------
            # Update Good Credit Customer Query
            # -----------------------------------
            c_data: str = ''

        customer_data['c_balance'] = c_balance
        customer_data['c_ytd_payment'] = c_ytd_payment
        customer_data['c_payment_cnt'] = c_payment_cnt
        customer_data['c_data'] = c_data

        await self.put(key, customer_data)

        # Concatenate w_name, four spaces, d_name
        h_data = "%s    %s" % (warehouse_data['w_name'], district_data['d_name'])

        # ----------------------
        # Insert History Query
        # ----------------------
        history_key = tuple_to_composite((w_id, d_id, key))
        history_params = {
            'h_c_id': key,
            'h_c_d_id': c_d_id,
            'h_c_w_id': c_w_id,
            'h_d_id': d_id,
            'h_w_id': w_id,
            'h_date': h_date,
            'h_amount': h_amount,
            'h_data': h_data,
        }

        await self.call_remote_function_no_response(
            'history',
            'InsertHistory',
            history_key,
            (history_key, history_params)
        )

        # TPC-C 2.5.3.3: Must display the following fields:
        # W_ID, D_ID, C_ID, C_D_ID, C_W_ID, W_STREET_1, W_STREET_2, W_CITY,
        # W_STATE, W_ZIP, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP,
        # C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE,
        # C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT,
        # C_BALANCE, the first 200 characters of C_DATA
        # (only if C_CREDIT = "BC"), H_AMOUNT, and H_DATE.

        # Hand back all the warehouse, district, and customer data
        return warehouse_data, district_data, customer_data
