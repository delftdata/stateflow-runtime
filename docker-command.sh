#!/bin/bash

set -m


exec python networking-service/tcp-server.py &

exec python universalis_operator/operator_server.py
