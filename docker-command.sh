#!/bin/bash

set -m

python networking-service/tcp-server.py &

fg %1