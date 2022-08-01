#!/bin/bash

exec docker compose -f docker-compose-kafka.yml down --volumes & docker compose down --volumes
exec docker compose -f docker-compose-kafka.yml up
