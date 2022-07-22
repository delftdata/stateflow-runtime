import asyncio
import logging

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
        customer_key = tuple_to_composite((w_id, d_id, c_id))

        warehouse_data = await self.call_remote_function_request_response(
            'warehouse',
            'GetWarehouse',
            warehouse_key,
            (warehouse_key,)
        )
        logging.warning(f'Got warehouse: {warehouse_data}')

        district = await self.get(key)
        logging.warning(f'Got district: {district}')

        customer_data = await self.call_remote_function_request_response(
            'customer',
            'GetCustomer',
            customer_key,
            (customer_key,)
        )
        logging.warning(f'Got customer: {customer_data}')

        w_tax = float(warehouse_data['w_tax'])
        d_tax = float(district['d_tax'])
        d_next_o_id = district['d_next_o_id']
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
        district['d_next_o_id'] = d_next_o_id

        # --------------------
        # Create Order query
        # --------------------
        order_params = {
            'o_id': d_next_o_id,
            'o_d_id': d_id,
            'o_w_id': w_id,
            'o_c_id': c_id,
            'o_entry_d': o_entry_d,
            'o_carrier_id': o_carrier_id,
            'o_ol_cnt': ol_cnt,
            'o_all_local': all_local
        }
        await self.call_remote_function_no_response('order', 'InsertOrder', order_key, (order_key, order_params,))
        logging.warning(f'Inserted order')

        # ------------------------
        # Create New Order Query
        # ------------------------
        new_order_params = {
            'no_o_id': d_next_o_id,
            'no_d_id': d_id,
            'no_w_id': w_id,
        }
        await self.call_remote_function_no_response(
            'new_order',
            'InsertNewOrder',
            new_order_key,
            (new_order_key, new_order_params,)
        )
        logging.warning(f'Inserted new order')

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
        stock_key = []

        for key, value in enumerate(i_ids):
            ol_number.append(key + 1)
            ol_supply_w_id.append(i_w_ids[key])
            ol_i_id.append(i_ids[key])
            ol_quantity.append(i_qtys[key])

            item_info = items[key]
            i_name.append(item_info['i_name'])
            i_data.append(item_info['i_data'])
            i_price.append(float(item_info['i_price']))

            # -----------------------------
            # Get Stock Information Query
            # -----------------------------
            stock_key.append(tuple_to_composite(([ol_supply_w_id][key], ol_i_id[key])))
            stock_info.append(await self.call_remote_function_no_response('stock', 'GetStock', stock_key, (stock_key,)))

        for key, value in enumerate(stock_info):
            s_quantity = float(value['s_quantity'])
            s_ytd = float(value['s_ytd'])
            s_order_cnt = float(value['s_order_cnt'])
            s_remote_cnt = float(value['s_remote_cnt'])
            s_data = value['s_data']
            s_dist_xx = value['S_DIST_' + str(d_id)]

            # --------------------
            # Update Stock Query
            # --------------------
            s_ytd += ol_quantity[key]
            if s_quantity >= ol_quantity[key] + 10:
                s_quantity = s_quantity - ol_quantity[key]
            else:
                s_quantity = s_quantity + 91 - ol_quantity[key]
            s_order_cnt += 1

            if ol_supply_w_id[key] != w_id:
                s_remote_cnt += 1

            stock_params = {
                's_quantity': s_quantity,
                's_ytd': s_ytd,
                's_order_cnt': s_order_cnt,
                's_remote_cnt': s_remote_cnt,
                's_data': s_data
            }
            await self.call_remote_function_no_response(
                'stock',
                'InsertStock',
                stock_key[key],
                (stock_key[key], stock_params)
            )

            if i_data[key].find(constants.ORIGINAL_STRING) != -1 and s_data.find(constants.ORIGINAL_STRING) != -1:
                brand_generic = 'B'
            else:
                brand_generic = 'G'

            # Transaction profile states to use "ol_quantity * i_price"
            ol_amount = ol_quantity[key] * i_price[key]
            total += ol_amount

            # -------------------------
            # Create Order Line Query
            # -------------------------
            order_line_key = tuple_to_composite((w_id, d_id, d_next_o_id, ol_number))
            order_line_params = {
                'ol_i_id': ol_i_id[key],
                'ol_supply_w_id': ol_supply_w_id[key],
                'ol_delivery_d': o_entry_d,
                'ol_quantity': ol_quantity[key],
                'ol_amount': ol_amount,
                'ol_dist_info': s_dist_xx
            }

            await self.call_remote_function_no_response(
                'order_line',
                'InsertOrderLine',
                order_line_key,
                (order_line_key, order_line_params)
            )
            item_data.append((i_name, s_quantity, brand_generic, i_price, ol_amount))

        # Adjust the total for the discount
        total *= (1 - c_discount) * (1 + w_tax + d_tax)

        # Pack up values the client is missing (see TPC-C 2.4.3.5)
        misc = w_tax, d_tax, d_next_o_id, total

        return (customer_data,) + misc + (item_data,)
