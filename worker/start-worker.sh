#!/bin/bash

set -m

exec python worker/worker_protocol.py &

exec redis-server /usr/local/etc/redis/redis.conf