import asyncio

from universalis.common.logging import logging
from universalis.common.stateful_function import StatefulFunction
from workloads.tpcc.util import constants
from workloads.tpcc.util.key import tuple_to_composite


class InsertDistrict(StatefulFunction):
    async def run(self, key: str, district: dict[str, int | float | str]):
        await self.put(key, district)
        return key, district


class GetDistrict(StatefulFunction):
    async def run(self, key: str):
        district = await self.get(key)
        return district


class NewOrder(StatefulFunction):
    async def run(self, key: str, params: dict):
        logging.warning('Running NewOrder')
        all_local = True

        # Initialize transaction parameters
        w_id: int = params['w_id']
        d_id: int = params['d_id']
        c_id: int = params['c_id']
        o_entry_d: str = params['o_entry_d']
        i_ids: list[int] = params['i_ids']
        i_w_ids: list[int] = params['i_w_ids']
        i_qtys: list[int] = params['i_qtys']

        # Validate transaction parameters
        assert len(i_ids) > 0
        assert len(i_ids) == len(i_w_ids)
        assert len(i_ids) == len(i_qtys)

        tasks = []
        for k, v in enumerate(i_ids):
            all_local = all_local and i_w_ids[k] == w_id

            i_key = str(v)
            tasks.append(self.call_remote_function_request_response('item', 'GetItem', i_key, (i_key,)))
        items = await asyncio.gather(*tasks)

        assert len(items) == len(i_ids)

        # --------------------
        # Get Customer, Warehouse and District information
        # --------------------
        warehouse_key = str(w_id)
        warehouse_data = await self.call_remote_function_request_response(
            'warehouse',
            'GetWarehouse',
            warehouse_key,
            (warehouse_key,)
        )

        logging.warning('Got Warehouse')

        district_key = tuple_to_composite((w_id, d_id))
        district_data = await self.call_remote_function_request_response(
            'district',
            'GetDistrict',
            district_key,
            (district_key,)
        )

        logging.warning('Got District')

        customer_key = tuple_to_composite((w_id, d_id, c_id))
        customer_data = await self.call_remote_function_request_response(
            'customer',
            'GetCustomer',
            customer_key,
            (customer_key,)
        )

        logging.warning('Got Customer')

        w_tax = float(warehouse_data['w_tax'])
        d_tax = float(district_data['d_tax'])
        d_next_o_id = district_data['d_next_o_id']
        c_discount = float(customer_data['c_discount'])

        # --------------------------
        # Insert Order Information
        # --------------------------
        ol_cnt = len(i_ids)
        o_carrier_id = 0o0
        order_key = tuple_to_composite((w_id, d_id, d_next_o_id))
        new_order_key = tuple_to_composite((w_id, d_id, d_next_o_id))

        # --------------------------
        # Increment next order id
        # --------------------------
        d_next_o_id += 1
        district_data['d_next_o_id'] = d_next_o_id

        # --------------------
        # Create Order query
        # --------------------
        order_params = {
            'o_c_id': c_id,
            'o_entry_d': o_entry_d,
            'o_carrier_id': o_carrier_id,
            'o_ol_cnt': ol_cnt,
            'o_all_local': all_local
        }
        await self.call_remote_function_no_response('order', 'InsertOrder', order_key, (order_key, order_params,))

        logging.warning('Inserted Order')

        # ------------------------
        # Create New Order Query
        # ------------------------
        new_order_params = {
            'no_w_id': w_id,
            'no_d_id': d_id,
            'no_o_id': d_next_o_id,
        }

        await self.call_remote_function_no_response(
            'new_order',
            'InsertNewOrder',
            new_order_key,
            (new_order_key, new_order_params,)
        )

        logging.warning('Inserted New Order')

        # -------------------------------
        # Insert Order Item Information
        # -------------------------------
        item_data = []
        total = 0

        ol_number = []
        ol_quantity = []
        ol_supply_w_id = []
        ol_i_id = []
        i_name = []
        i_price = []
        i_data = []
        stock_info = []
        stock_keys = []

        for k, v in enumerate(i_ids):
            ol_number.append(k + 1)
            ol_supply_w_id.append(i_w_ids[k])
            ol_i_id.append(i_ids[k])
            ol_quantity.append(i_qtys[k])

            item_info = items[k]
            i_name.append(item_info['i_name'])
            i_data.append(item_info['i_data'])
            i_price.append(float(item_info['i_price']))

            # -----------------------------
            # Get Stock Information Query
            # -----------------------------
            stock_key = tuple_to_composite((ol_supply_w_id[k], ol_i_id[k]))
            stock_keys.append(stock_key)

            stock_info.append(
                await self.call_remote_function_request_response(
                    'stock',
                    'GetStock',
                    stock_key,
                    (stock_key,)
                )
            )

        for k, v in enumerate(stock_info):
            s_quantity = float(v['s_quantity'])
            s_ytd = float(v['s_ytd'])
            s_order_cnt = float(v['s_order_cnt'])
            s_remote_cnt = float(v['s_remote_cnt'])
            s_data = v['s_data']
            s_dist_xx = v['s_dist_' + str(d_id)]

            # --------------------
            # Update Stock Query
            # --------------------
            s_ytd += ol_quantity[k]
            if s_quantity >= ol_quantity[k] + 10:
                s_quantity = s_quantity - ol_quantity[k]
            else:
                s_quantity = s_quantity + 91 - ol_quantity[k]
            s_order_cnt += 1

            if ol_supply_w_id[k] != w_id:
                s_remote_cnt += 1

            v['s_quantity']: s_quantity
            v['s_ytd']: s_ytd
            v['s_order_cnt']: s_order_cnt
            v['s_remote_cnt']: s_remote_cnt
            v['s_data']: s_data
            v['s_dist_' + str(d_id)]: s_dist_xx

            await self.call_remote_function_no_response(
                'stock',
                'InsertStock',
                stock_keys[k],
                (stock_keys[k], v)
            )

            logging.warning('Inserted Stock')

            if i_data[k].find(constants.ORIGINAL_STRING) != -1 and s_data.find(constants.ORIGINAL_STRING) != -1:
                brand_generic = 'B'
            else:
                brand_generic = 'G'

            # Transaction profile states to use "ol_quantity * i_price"
            ol_amount = ol_quantity[k] * i_price[k]
            total += ol_amount

            # -------------------------
            # Create Order Line Query
            # -------------------------
            order_line_key = tuple_to_composite((w_id, d_id, d_next_o_id, ol_number))
            order_line_params = {
                'ol_i_id': ol_i_id[k],
                'ol_supply_w_id': ol_supply_w_id[k],
                'ol_delivery_d': o_entry_d,
                'ol_quantity': ol_quantity[k],
                'ol_amount': ol_amount,
                'ol_dist_info': s_dist_xx
            }

            await self.call_remote_function_no_response(
                'order_line',
                'InsertOrderLine',
                order_line_key,
                (order_line_key, order_line_params)
            )

            logging.warning('Inserted OrderLine')

            item_data.append((i_name, s_quantity, brand_generic, i_price, ol_amount))

        # Adjust the total for the discount
        total *= (1 - c_discount) * (1 + w_tax + d_tax)

        # Pack up values the client is missing (see TPC-C 2.4.3.5)
        misc = w_tax, d_tax, d_next_o_id, total

        return (customer_data,) + misc + (item_data,)
