FROM apache/airflow:2.3.4-python3.10

USER airflow

COPY profiles.yml requirements.txt ./modules/*.whl ./dashboard_config/*.zip ./
COPY dags_factory /opt/airflow/dags_factory/
COPY sql_queris /opt/airflow/sql_queris/
COPY utils /opt/airflow/utils/

RUN pip install --no-cache-dir --user -r requirements.txt
RUN pip install --no-cache-dir --user my_target-0.0.0-py3-none-any.whl
RUN pip install --no-cache-dir --user yandex_direct-0.0.0-py3-none-any.whl
RUN pip install --no-cache-dir --user superset_connector-0.0.0-py3-none-any.whl
