FROM apache/airflow:2.3.4-python3.10

#USER root
#
#RUN apt-get update \
#    && apt-get install -y clickhouse-client zip unzip wget curl

USER airflow

COPY requirements.txt ./modules/*.whl ./
COPY dags_factory /opt/airflow/dags_factory/
COPY sql_queris /opt/airflow/sql_queris/
COPY utils /opt/airflow/utils/

RUN pip install --no-cache-dir --user -r requirements.txt
RUN pip install --no-cache-dir --user my_target-0.0.0-py3-none-any.whl
RUN pip install --no-cache-dir --user yandex_direct-0.0.0-py3-none-any.whl
