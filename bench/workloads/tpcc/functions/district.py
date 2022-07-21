import asyncio

from universalis.common.logging import logging
from universalis.common.stateful_function import StatefulFunction

from workloads.tpcc.functions import order_line, stock, new_order
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
    async def run(self, params: dict):
        all_local = True
        tasks = []

        # Validate transaction parameters
        assert len(params['i_ids']) > 0
        assert len(params['i_ids']) == len(params['i_w_ids'])
        assert len(params['i_ids']) == len(params['i_qtys'])

        items = []
        for k, v in enumerate(params['i_ids']):
            all_local = all_local and v == params['w_id']
            logging.warning(f'Getting {v}')
            data = await self.call_remote_function_request_response('item', 'Get', v, (v,))
            logging.warning(f'Got {data}')
            items.append(data)

        assert len(items) == len(params['i_ids'])
        logging.warning(items)
        tasks = []

        # --------------------
        # Get Customer, Warehouse and District information
        # --------------------
        warehouse_key = params['w_id']
        district_key = tuple_to_composite((params['w_id'], params['d_id']))
        customer_key = tuple_to_composite((params['w_id'], params['d_id'], params['c_id']))

        tasks.append(self.call_remote_function_request_response('warehouse', 'Get', warehouse_key, (warehouse_key,)))
        tasks.append(self.call_remote_function_request_response('district', 'Get', district_key, (district_key,)))
        tasks.append(self.call_remote_function_request_response('customer', 'Get', customer_key, (customer_key,)))

        [warehouse, district, customer] = await asyncio.gather(*tasks)
        tasks = []

        w_tax = float(warehouse['w_tax'])
        d_tax = float(district['d_tax'])
        d_next_o_id = district['d_next_o_id']
        customer_info = customer
        c_discount = float(customer['c_discount'])

        # --------------------------
        # Insert Order Information
        # --------------------------
        ol_cnt = len(params['i_ids'])
        o_carrier_id = 0o0
        order_key = tuple_to_composite((params['w_id'], params['d_id'], d_next_o_id))
        new_order_key = tuple_to_composite((params['w_id'], params['d_id'], d_next_o_id))

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
            'o_d_id': params['d_id'],
            'o_w_id': params['w_id'],
            'o_c_id': params['c_id'],
            'o_entry_d': params['o_entry_d'],
            'o_carrier_id': o_carrier_id,
            'o_ol_cnt': ol_cnt,
            'o_all_local': all_local
        }
        tasks.append(self.call_remote_function_no_response('order', 'Insert', order_key, (order_key, order_params,)))

        # ------------------------
        # Create New Order Query
        # ------------------------
        new_order_params = {
            'no_o_id': d_next_o_id,
            'no_d_id': params['d_id'],
            'no_w_id': params['w_id'],
        }
        tasks.append(
            self.call_remote_async(new_order.InsertNewOrder, new_order_key, (new_order_key, new_order_params,))
            )

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
        stock_tasks = []
        stock_key = []

        for k, v in enumerate(params['i_ids']):
            ol_number.append(k + 1)
            ol_supply_w_id.append(params['i_w_ids'][k])
            ol_i_id.append(params['i_ids'][k])
            ol_quantity.append(params['i_qtys'][k])

            item_info = items[k]
            i_name.append(item_info['i_name'])
            i_data.append(item_info['i_data'])
            i_price.append(float(item_info['i_price']))

            # -----------------------------
            # Get Stock Information Query
            # -----------------------------
            stock_key.append(tuple_to_composite(([ol_supply_w_id][k], ol_i_id[k])))
            stock_tasks.append(self.call_remote_async(stock.GetStock, stock_key, (stock_key,)))

        stock_info = await asyncio.gather(*stock_tasks)
        for k, v in enumerate(stock_info):
            s_quantity = float(v['s_quantity'])
            s_ytd = float(v['s_ytd'])
            s_order_cnt = float(v['s_order_cnt'])
            s_remote_cnt = float(v['s_remote_cnt'])
            s_data = v['s_data']
            s_dist_xx = v['S_DIST_' + str(params['d_id'])]

            # --------------------
            # Update Stock Query
            # --------------------
            s_ytd += ol_quantity[k]
            if s_quantity >= ol_quantity[k] + 10:
                s_quantity = s_quantity - ol_quantity[k]
            else:
                s_quantity = s_quantity + 91 - ol_quantity[k]
            s_order_cnt += 1

            if ol_supply_w_id[k] != params['w_id']:
                s_remote_cnt += 1

            stock_params = {
                's_quantity': s_quantity,
                's_ytd': s_ytd,
                's_order_cnt': s_order_cnt,
                's_remote_cnt': s_remote_cnt,
                's_data': s_data
            }
            tasks.append(self.call_remote_async(stock.InsertStock, stock_key[k], (stock_key[k], stock_params)))

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
            order_line_key = tuple_to_composite((params['w_id'], params['d_id'], d_next_o_id, ol_number))
            order_line_params = {
                'ol_i_id': ol_i_id[k],
                'ol_supply_w_id': ol_supply_w_id[k],
                'ol_delivery_d': params['o_entry_d'],
                'ol_quantity': ol_quantity[k],
                'ol_amount': ol_amount,
                'ol_dist_info': s_dist_xx
            }
            tasks.append(
                self.call_remote_async(order_line.InsertOrderLine, order_line_key, (order_line_key, order_line_params))
                )

            item_data.append((i_name, s_quantity, brand_generic, i_price, ol_amount))

        # Commit all updates
        await asyncio.gather(*tasks)

        # Adjust the total for the discount
        total *= (1 - c_discount) * (1 + w_tax + d_tax)

        # Pack up values the client is missing (see TPC-C 2.4.3.5)
        misc = w_tax, d_tax, d_next_o_id, total

        return (customer_info,) + misc + (item_data,)
