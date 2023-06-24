from dataclasses import asdict, fields
from typing import Union, Optional

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.http.sensors.http import HttpSensor
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator

from utils.utils import sql_file_reader
from yandex_direct.yandex_direct_to_s3_operator import YandexDirectToS3Operator
from yandex_direct.direct import Direct


def yandex_direct_dags_factory(
        dag_id: str,
        schedule_interval: str,
        default_args: dict,
        yandex_direct_conn_id: str,
        s3_conn_id: str,
        ch_conn_id: str,
        yandex_direct_fields_dataclass: Direct(),
        yandex_direct_report_type: str,
        s3_bucket: str,
        project_name: str,
        sql_query_filename: str,
        ch_database_name: str,
        ch_table_name: str,
        max_active_runs: int = 1,
        etl_start_date: str = "{{ (data_interval_end - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}",
        etl_end_date: Optional[str] = None,
        tags: Union[list] = None,
        on_failure_callback=None,
        on_success_callback=None,
) -> DAG:
    """
    Function for creating DAG fabric for Yandex Direct

    :param dag_id: DAG id
    :param schedule_interval: Schedule interval
    :param default_args: Default args for DAG
    :param yandex_direct_conn_id: yarn direct connection id from Airflow
    :param s3_conn_id: S3 connection id from Airflow
    :param ch_conn_id: Clickhouse connection id from Airflow
    :param yandex_direct_fields_dataclass: Yandex direct fields dataclass like Direct() with init params
    :param yandex_direct_report_type: Report type for Yandex Direct API. Can be 'ACCOUNT_PERFORMANCE_REPORT',
        'AD_PERFORMANCE_REPORT', 'AD_GROUP_PERFORMANCE_REPORT', 'CAMPAIGN_PERFORMANCE_REPORT',
        'CRITERIA_PERFORMANCE_REPORT', 'CUSTOM_REPORT', 'REACH_AND_FREQUENCY_PERFORMANCE_REPORT',
        'SEARCH_QUERY_PERFORMANCE_REPORT'.
    :param s3_bucket: S3 bucket name
    :param project_name: Project name
    :param sql_query_filename: sql query filename with extension .sql
    :param ch_database_name: Clickhouse database name
    :param ch_table_name: Clickhouse table name
    :param max_active_runs: Max active runs for DAG
    :param etl_start_date: Start date for ETL process in Jinja2 format
    :param etl_end_date: End date for ETL process in Jinja2 format
    :param tags: Tags for DAG
    :param on_failure_callback: Callback function for DAG failure
    :param on_success_callback: Callback function for DAG success
    :return: DAG
    """
    if not etl_end_date:
        etl_end_date = etl_start_date
    with DAG(
            dag_id=dag_id,
            description='Load yandex direct data from source to s3',
            default_args=default_args,
            schedule_interval=schedule_interval,
            max_active_runs=max_active_runs,
            tags=tags,
            on_success_callback=on_success_callback,
            on_failure_callback=on_failure_callback
    ) as dag:
        def response_check(response):
            if response:
                return True
            else:
                return False

        check_direct_status_ = HttpSensor(
            task_id='check_yandex_direct_status',
            http_conn_id='yandex_direct',
            endpoint='#gruppa-15',
            response_check=response_check
        )

        yandex_direct_to_s3_ = YandexDirectToS3Operator(
            task_id='yandex_direct_to_s3',
            yandex_direct_conn_id=yandex_direct_conn_id,
            s3_conn_id=s3_conn_id,
            s3_bucket=s3_bucket,
            s3_key=f'yandex_direct/{project_name}/{yandex_direct_report_type}_{etl_start_date}_{etl_end_date}.tsv',
            yandex_direct_fields=[x for x in asdict(yandex_direct_fields_dataclass).values()],
            yandex_direct_report_type=yandex_direct_report_type,
            yandex_direct_start_date=etl_start_date,
            yandex_direct_end_date=etl_end_date,
        )

        create_ch_table_ = ClickHouseOperator(
            task_id=f'create_ch_table_{ch_table_name}',
            clickhouse_conn_id=ch_conn_id,
            sql=f"""create table if not exists {ch_table_name} (
                {', '.join([f'`{col.default}` {col.type}' for col in fields(yandex_direct_fields_dataclass)])},
                partition UInt32,
                create_datetime Datetime default now()
            )
            engine = MergeTree()
            order by Date
            partition by partition
            """,
            database=ch_database_name
        )

        def generate_drop_partition_sql() -> str:
            """Generate sql for drop partition if etl_end_date is not None"""
            if etl_end_date == etl_start_date:
                return f"alter table {ch_table_name} drop partition {etl_start_date.replace('%Y-%m-%d', '%Y%m%d')}"
            else:
                return f"""alter table {ch_table_name} 
                            delete where partition 
                            between {etl_start_date.replace('%Y-%m-%d', '%Y%m%d')} 
                            and {etl_end_date.replace('%Y-%m-%d', '%Y%m%d')}"""

        drop_partition_ = ClickHouseOperator(
            task_id=f'drop_partition',
            clickhouse_conn_id=ch_conn_id,
            sql=generate_drop_partition_sql(),
            database=ch_database_name,
        )

        insert_into_clickhouse_ = ClickHouseOperator(
            task_id=f'insert_into_clickhouse_{ch_table_name}',
            clickhouse_conn_id=ch_conn_id,
            sql=sql_file_reader(project_name=project_name,
                                query_filename=sql_query_filename,
                                table=ch_table_name,
                                s3_bucket=s3_bucket,
                                project=project_name,
                                report_type=yandex_direct_report_type,
                                s3_access_key=BaseHook.get_connection(s3_conn_id).extra_dejson.get("aws_access_key_id"),
                                s3_secret_key=BaseHook.get_connection(s3_conn_id).extra_dejson.get(
                                    "aws_secret_access_key"),
                                start_date=etl_start_date,
                                end_date=etl_end_date,
                                ),
            database=ch_database_name)

        check_direct_status_ >> yandex_direct_to_s3_ >> create_ch_table_ >> drop_partition_ >> insert_into_clickhouse_

    return dag
