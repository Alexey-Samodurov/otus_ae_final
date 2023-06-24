import os
import uuid
from datetime import datetime

import pendulum
from airflow import settings
from airflow.models import Connection

from dags_factory.my_target_dag_factory import my_target_dag_factory
from dags_factory.yandex_direct_dags_factory import yandex_direct_dags_factory
from yandex_direct.direct import Direct

LOCAL_TZ = pendulum.timezone('Europe/Moscow')
DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 5, tzinfo=LOCAL_TZ),
    'end_date': datetime(2023, 6, 5, tzinfo=LOCAL_TZ),
    'depends_on_past': False,
    'catchup': True
}
DATABASE = 'TEST'

my_target_conn = Connection(
    conn_id='my_target_api',
    conn_type='http',
    extra={
        "client_id": os.environ['MY_TARGET_CLIENT_ID'],
        "client_secret": os.environ['MY_TARGET_CLIENT_SECRET']
    }
)

my_target_url = Connection(
    conn_id='my_target',
    conn_type='http',
    host='https://target.my.com/'
)

yandex_direct_conn = Connection(
    conn_id='yandex_direct_api',
    conn_type='http',
    extra={
        "token": os.environ['YANDEX_DIRECT_TOKEN']
    }
)

yandex_direct_url = Connection(
    conn_id='yandex_direct',
    conn_type='http',
    host='https://direct.yandex.ru/'
)

s3_conn = Connection(
    conn_id='s3',
    conn_type='s3',
    extra={
        "aws_access_key_id": os.environ['S3_KEY'],
        "aws_secret_access_key": os.environ['S3_SECRET'],
        "endpoint_url": 'https://storage.yandexcloud.net'
    }
)

clickhouse_conn = Connection(
    conn_id='clickhouse',
    conn_type='sqlite',
    host='clickhouse',
    port=9000,
    login='default'
)

for conn in [my_target_conn, my_target_url, yandex_direct_conn, yandex_direct_url, s3_conn, clickhouse_conn]:
    with settings.Session() as session:
        if not session.query(Connection).filter(Connection.conn_id == conn.conn_id).first():
            session.add(conn)
            session.commit()

FACTORIES_CFG = {
    yandex_direct_dags_factory: {
        "dag_id": "yandex_direct_custom_report_dinamic_factory",
        "schedule_interval": "@daily",
        "default_args": DEFAULT_ARGS,
        "yandex_direct_conn_id": yandex_direct_conn.conn_id,
        "s3_conn_id": s3_conn.conn_id,
        "ch_conn_id": clickhouse_conn.conn_id,
        "yandex_direct_fields_dataclass": Direct(),
        "yandex_direct_report_type": "CUSTOM_REPORT",
        "s3_bucket": "otus-final",
        "project_name": "yandex_direct",
        "sql_query_filename": 'yandex_direct_custom_report.sql',
        'ch_database_name': DATABASE,
        'ch_table_name': 'yandex_direct_custom_report',
        'tags': ['samodurov'],
    },
    my_target_dag_factory: {
        "dag_id": "my_target_custom_report_dinamic_factory",
        "schedule_interval": "@daily",
        "default_args": DEFAULT_ARGS,
        'mt_conn_id': my_target_conn.conn_id,
        'ch_conn_id': clickhouse_conn.conn_id,
        "s3_bucket": "otus-final",
        's3_conn_id': s3_conn.conn_id,
        'project_name': 'my_target',
        'sql_query_metrics_filename': 'my_target_metrics.sql',
        'sql_query_dimensions_filename': 'my_target_dimensions.sql',
        'ch_database_name': DATABASE,
        'mt_type_of_data': 'ad_groups',
        'tags': ['samodurov'],
    }}

for factory, kwargs in FACTORIES_CFG.items():
    locals()[str(uuid.uuid4())] = factory(**kwargs)


