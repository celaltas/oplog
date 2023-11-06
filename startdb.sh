#!/bin/bash

docker compose up -d

sleep 5

docker exec mongo /scripts/rs-init.sh