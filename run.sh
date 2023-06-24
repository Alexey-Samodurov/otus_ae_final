#!/usr/bin/env bash

mkdir ./airflow_logs
mkdir ./airflow_plugins
docker-compose -f docker-compose-airflow.yaml up airflow-init --build
docker-compose -f docker-compose-airflow.yaml up -d
docker-compose -f docker-compose-superset.yaml up superset-init --build
docker-compose -f docker-compose-superset.yaml up -d
