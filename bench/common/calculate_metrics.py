import math
import os
import pathlib
import re

import numpy as np
import pandas as pd

from common.logging import logging


def calculate():
    results_dir = './results'

    requests_filename = os.path.join(results_dir, 'requests.csv')
    responses_filename = os.path.join(results_dir, 'responses.csv')

    logging.info('Calculating Metrics')

    requests = pd.read_csv(requests_filename)
    os.remove(requests_filename)

    responses = pd.read_csv(responses_filename)
    os.remove(responses_filename)

    merged = pd.merge(requests, responses, on='request_id', how='outer').dropna()
    merged['latency'] = merged['timestamp_y'] - merged['timestamp_x']
    latency = merged['latency']

    logging.info(f'min latency: {min(latency)}ms')
    logging.info(f'max latency: {max(latency)}ms')
    logging.info(f'average latency: {np.average(latency)}ms')
    logging.info(f'99%: {np.percentile(latency, 99)}ms')
    logging.info(f'95%: {np.percentile(latency, 95)}ms')
    logging.info(f'90%: {np.percentile(latency, 90)}ms')
    logging.info(f'75%: {np.percentile(latency, 75)}ms')
    logging.info(f'60%: {np.percentile(latency, 60)}ms')
    logging.info(f'50%: {np.percentile(latency, 50)}ms')
    logging.info(f'25%: {np.percentile(latency, 25)}ms')
    logging.info(f'10%: {np.percentile(latency, 10)}ms')

    latencies = merged[['request_id', 'latency']]
    latencies_filename = results_dir + '/latencies.csv'
    latencies.to_csv(latencies_filename, index=False)

    start_time = -math.inf
    bucket_id = -1
    max_latency_threshold = np.percentile(latency, 90)
    granularity = 1000  # 1 second (ms) (i.e. bucket size)
    throughput: dict[int, int] = {}

    for index, row in merged.iterrows():
        if row['timestamp_y'] - start_time > granularity:
            bucket_id += 1
            start_time = row['timestamp_y']
            throughput[bucket_id] = 1
        elif row['latency'] <= max_latency_threshold:
            throughput[bucket_id] += 1

    throughputs = pd.DataFrame(data=throughput.items(), columns=['second', 'throughput'])
    throughputs_filename = os.path.join(results_dir, 'throughputs.csv')
    throughputs.to_csv(throughputs_filename, index=False)
    tp = throughputs['throughput']

    logging.info(f'max throughput: {max(tp)}')
    logging.info(f'average throughput: {np.average(tp)}')

    worker_abort_rate_files = pathlib.Path(results_dir)
    worker_abort_rates = {}

    for worker_file in worker_abort_rate_files.glob('abort_rates_worker_[0-9]*.csv'):
        try:
            worker_id = re.findall('\d+', worker_file.stem)[0]
            worker_abort_rates[worker_id] = pd.read_csv(worker_file)

        except AttributeError:
            logging.info(f'{worker_file} could not be processed')
        finally:
            os.remove(worker_file)

    abort_rates = pd.concat(worker_abort_rates) \
        .rename_axis(['id', None]) \
        .reset_index(level='id') \
        .rename(columns={'id': 'worker_id'})

    abort_rate_filename = os.path.join(results_dir, 'abort_rates.csv')
    abort_rates.to_csv(abort_rate_filename, index=False)

    average = np.average(abort_rates['abort_rate'])
    logging.info(f'average abort rate: {np.average(average)}')
