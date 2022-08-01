#!/bin/bash

exec docker compose up --build --scale worker=2
