#!/bin/bash

set -m


exec python networking-service/tcp-server.py &

exec python worker/operator_server.py
