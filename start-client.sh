#!/bin/bash

exec docker-compose -f docker-compose-client.yml up --force-recreate --build