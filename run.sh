#!/usr/bin/env bash

mkdir ./airflow_logs
mkdir ./airflow_plugins
docker-compose up airflow-init --build
docker-compose up superset-init --build
docker-compose up -d
echo Wait for connections ready...
sleep 30
echo Airflow ready on: http://localhost:8080
echo Superset ready on: http://localhost:8088
