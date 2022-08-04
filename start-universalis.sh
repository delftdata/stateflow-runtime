#!/bin/bash

exec docker-compose up --force-recreate --build --scale worker="${1:-2}"
