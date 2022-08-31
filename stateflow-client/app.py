import asyncio
import os
import random
import time
import pandas as pd

from sanic import Sanic
from sanic.response import json
from stateflow.client.universalis_client import UniversalisClient

from stateflow.runtime.universalis.universalis_runtime import UniversalisRuntime
from universalis.common.stateflow_ingress import IngressTypes

import demo_ycsb
from demo_ycsb import YCSBEntity, stateflow
from zipfian_generator import ZipfGenerator
from universalis.universalis import Universalis

app = Sanic(__name__)

UNIVERSALIS_HOST: str = os.environ['UNIVERSALIS_HOST']
UNIVERSALIS_PORT: int = int(os.environ['UNIVERSALIS_PORT'])
KAFKA_URL: str = os.environ['KAFKA_URL']
N_PARTITIONS: int = int(os.environ['N_PARTITIONS'])


N_ENTITIES = int(os.environ['N_ENTITIES'])
keys: list[int] = list(range(N_ENTITIES))
STARTING_AMOUNT = int(os.environ['STARTING_AMOUNT'])
N_TASKS = int(os.environ['N_TASKS'])
WORKLOAD = os.environ['WORKLOAD']
RPS = int(os.environ['RPS'])


entities = {}


@app.post('/hello')
async def hello(_):
    return json('Hey', status=200)


@app.post('/submit_dataflow_graph')
async def submit_dataflow_graph(_):
    print("INIT STARTING")
    app.ctx.universalis = Universalis(UNIVERSALIS_HOST, UNIVERSALIS_PORT, IngressTypes.KAFKA,
                                              kafka_url=KAFKA_URL)

    app.ctx.flow = stateflow.init()

    app.ctx.runtime = UniversalisRuntime(app.ctx.flow,
                                                 app.ctx.universalis,
                                                 "Stateflow",
                                                 n_partitions=N_PARTITIONS)
    print("INIT DONE")
    universalis_operators = await app.ctx.runtime.run((demo_ycsb,))
    app.ctx.client = UniversalisClient(flow=app.ctx.flow,
                                       universalis_client=app.ctx.universalis,
                                       kafka_url=KAFKA_URL,
                                       operators=universalis_operators)
    return json('Graph submitted', status=200)


@app.post('/init_entities')
async def init_entities(_):
    for i in keys:
        print(f'Creating: {i}')
        entities[i] = YCSBEntity(str(i), STARTING_AMOUNT).get()
    app.ctx.client.stop_consumer_thread()
    return json('Entities initialized', status=200)


@app.post('/start_benchmark')
async def start_benchmark(_):
    app.ctx.client.start_result_consumer_process()
    zipf_gen = ZipfGenerator(items=N_ENTITIES)
    operations = ["r", "u", "t"]
    operation_mix_a = [0.5, 0.5, 0.0]
    operation_mix_b = [0.95, 0.05, 0.0]
    operation_mix_t = [0.0, 0.0, 1.0]
    operation_mix_r = [1.0, 0.0, 0.0]
    operation_mix_w = [0.0, 1.0, 0.0]
    if WORKLOAD == 'a':
        operation_mix = operation_mix_a
    elif WORKLOAD == 'b':
        operation_mix = operation_mix_b
    elif WORKLOAD == 'r':
        operation_mix = operation_mix_r
    elif WORKLOAD == 'w':
        operation_mix = operation_mix_w
    else:
        operation_mix = operation_mix_t
    await asyncio.sleep(10)
    sleep_time = (1000 / RPS) // 1000  # sec
    for _ in range(N_TASKS):
        key = keys[next(zipf_gen)]
        op = random.choices(operations, weights=operation_mix, k=1)[0]
        if op == "r":
            entities[key].read()
        elif op == "u":
            entities[key].update(STARTING_AMOUNT)
        else:
            key2 = keys[next(zipf_gen)]
            while key2 == key:
                key2 = keys[next(zipf_gen)]
            entities[key].transfer(1, entities[key2])
        await asyncio.sleep(sleep_time)
    app.ctx.client.store_request_csv()
    await asyncio.sleep(10)
    print("Stopping")
    app.ctx.client.stop_result_consumer_process()
    await asyncio.sleep(10)
    response = {"in": pd.read_csv('universalis_client_requests.csv').to_dict(),
                "out": pd.read_csv('output.csv').to_dict()}
    return json(response, status=200)


if __name__ == '__main__':
    app.run(debug=False)
