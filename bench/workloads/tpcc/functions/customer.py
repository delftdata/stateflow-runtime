from universalis.common.stateful_function import StatefulFunction


class InvalidItemId(Exception):
    pass


class Insert(StatefulFunction):
    async def run(self, key: str, customer: dict[str, int | float | str]):
        await self.put(key, customer)
        return key, customer


class NewOrder(StatefulFunction):
    async def run(self, key: str, params: dict):
        all_local = True
        items = []

        # Validate transaction parameters
        assert len(params['i_ids']) > 0
        assert len(params['i_ids']) == len(params['i_w_ids'])
        assert len(params['i_ids']) == len(params['i_qtys'])

        for k, v in enumerate(params['i_ids']):
            all_local = all_local and v == params['w_id']
            items.append(await self.call_remote_function_request_response('item', 'Get', v, (v,)))
        assert len(items) == len(params['i_ids'])

        return items


class Payment(StatefulFunction):
    async def run(self, key: str, params: tuple):
        pass
