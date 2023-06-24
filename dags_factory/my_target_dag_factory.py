from functools import reduce
from typing import Union, Optional

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.task_group import TaskGroup
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from my_target.my_target_dimensions_to_s3_operator import MyTargetDimensionsToS3Operator
from my_target.my_target_to_s3_operator import MyTargetToS3Operator

from utils.utils import sql_file_reader


def my_target_dag_factory(
        dag_id: str,
        mt_conn_id: str,
        ch_conn_id: str,
        s3_conn_id: str,
        s3_bucket: str,
        schedule_interval: Optional[str],
        default_args: dict,
        project_name: str,
        sql_query_metrics_filename: str,
        sql_query_dimensions_filename: str,
        ch_database_name: str,
        mt_type_of_data: str,
        mt_metrics: Union[str, list, tuple] = 'all',
        mt_info_dimensions: Union[str, list, tuple] = 'ad_groups',
        max_active_runs: int = 1,
        etl_start_date: str = "{{ (data_interval_end - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}",
        etl_end_date: Optional[str] = None,
        tags: Union[list] = None,
        on_failure_callback=None,
        on_success_callback=None,
) -> DAG:
    """
    Function for creating DAG fabric for MyTarget API. Create table in Clickhouse, get data from API, transform and
    insert into Clickhouse as s3 format.

    :param dag_id: DAG id
    :param mt_conn_id: My Target connection id in Airflow
    :param ch_conn_id: Clickhouse connection id in Airflow
    :param s3_conn_id: S3 connection id in Airflow
    :param s3_bucket: S3 bucket name
    :param schedule_interval: DAG schedule interval
    :param default_args: DAG default args
    :param project_name: Project name
    :param sql_query_metrics_filename: SQL query filename for metrics
    :param sql_query_dimensions_filename: SQL query filename for dimensions
    :param ch_database_name: Clickhouse database name
    :param mt_type_of_data: Type of data for MyTarget can bee campaigns, banners or users
    :param mt_metrics: String, list or tuple of metrics need to collect from API, can be like
        all, base, events, uniques, video, carousel, tps, moat, playable, romi, ad_offers
    :param mt_info_dimensions: String, list or tuple of dimensions need to collect from API, can be like campaigns_info,
    banners_info, packages_pads_info
    :param max_active_runs: Max active runs
    :param etl_start_date: ETL start date by default today - 1 day, in format '%Y-%m-%d'
    :param etl_end_date: ETL end date by default equal to etl_start_date, in format '%Y-%m-%d'
    :param tags: DAG tags
    :param on_failure_callback: callback for failure
    :param on_success_callback: callback for success
    :return: DAG
    """
    if not etl_end_date:
        etl_end_date = etl_start_date
    with DAG(
            dag_id=dag_id,
            description='Проверям доступность МТ, парсим данные с МТ за день -1, трансформируем, заливаем в clickhouse',
            default_args=default_args,
            on_failure_callback=on_failure_callback,
            on_success_callback=on_success_callback,
            schedule_interval=schedule_interval,
            max_active_runs=max_active_runs,
            tags=tags,
    ) as dag:
        if isinstance(mt_metrics, str):
            mt_metrics = [mt_metrics]
        if isinstance(mt_info_dimensions, str):
            mt_info_dimensions = [mt_info_dimensions]

        def response_check(response):
            if response:
                return True
            else:
                return False

        check_my_target_status_ = HttpSensor(
            task_id='check_my_target_status',
            http_conn_id='my_target',
            endpoint='login/',
            response_check=response_check
        )

        def create_table_columns(metric_name: str) -> str:
            from dataclasses import fields
            from my_target.schemas_matcher import match_dict

            for cls, name in match_dict.items():
                if name == metric_name:
                    return ', '.join([f'{col.name} {col.type}' for col in fields(cls())])

        with TaskGroup(group_id='create_tables') as create_tables:
            create_tables_list = []
            for metric in mt_metrics:
                columns = create_table_columns(metric_name=metric)
                create_table_ = ClickHouseOperator(
                    task_id=f'create_table_{metric}',
                    clickhouse_conn_id=ch_conn_id,
                    database=ch_database_name,
                    sql="""CREATE TABLE IF NOT EXISTS 
                    {{ params.project_name }}_{{ params.mt_type_of_data }}_{{ params.metric }} (
                    {{ params.columns }},
                    partition UInt32,
                    create_datetime DateTime
                    )
                    ENGINE = MergeTree()
                    partition by partition
                    order by partition
                    """,
                    params={'project_name': project_name,
                            'mt_type_of_data': mt_type_of_data,
                            'metric': metric,
                            'columns': columns},
                )
                create_tables_list.append(create_table_)
            for dimension in mt_info_dimensions:
                columns = create_table_columns(metric_name=dimension)
                create_info_dimension_ = ClickHouseOperator(
                    task_id=f'create_info_dimension_{dimension}',
                    clickhouse_conn_id=ch_conn_id,
                    database=ch_database_name,
                    sql="""CREATE TABLE IF NOT EXISTS 
                    {{ params.project_name }}_{{ params.dimension }} (
                    {{ params.columns }},
                    create_datetime DateTime
                    )
                    ENGINE = MergeTree()
                    order by tuple()
                    """,
                    params={'project_name': project_name,
                            'dimension': dimension,
                            'columns': columns},
                )
                create_tables_list.append(create_info_dimension_)
            reduce(lambda x, y: [x, y], create_tables_list)

        with TaskGroup(group_id='drop_metrics_partitions') as drop_metrics_partitions:
            drop_partitions_list = []
            for metric in mt_metrics:
                drop_partition_ = ClickHouseOperator(
                    task_id=f'drop_partition_{metric}',
                    clickhouse_conn_id=ch_conn_id,
                    database=ch_database_name,
                    sql="""alter table {{ params.project_name }}_{{ params.mt_type_of_data }}_{{ params.metric }} 
                    drop partition {{ (data_interval_end - macros.timedelta(days=1)).strftime('%Y%m%d') }};""",
                    params={'project_name': project_name,
                            'mt_type_of_data': mt_type_of_data,
                            'metric': metric,
                            },
                )
                drop_partitions_list.append(drop_partition_)
            reduce(lambda x, y: [x, y], drop_partitions_list)

        with TaskGroup(group_id='truncate_info_dimensions') as truncate_info_dimensions:
            truncate_info_dimensions_list = []
            for dimension in mt_info_dimensions:
                truncate_info_dimensions_ = ClickHouseOperator(
                    task_id=f'truncate_info_dimensions_{dimension}',
                    clickhouse_conn_id=ch_conn_id,
                    database=ch_database_name,
                    sql="truncate table {{ params.project_name }}_{{ params.dimension }};",
                    params={'project_name': project_name, 'dimension': dimension},
                )
                truncate_info_dimensions_list.append(truncate_info_dimensions_)
            reduce(lambda x, y: [x, y], truncate_info_dimensions_list)

        with TaskGroup(group_id='save_metrics_data') as save_metrics_data:
            save_data_to_s3_list = []
            for metric in mt_metrics:
                save_data_to_s3_ = MyTargetToS3Operator(
                    task_id=f'save_data_to_s3_{metric}',
                    my_target_conn_id=mt_conn_id,
                    s3_conn_id=s3_conn_id,
                    s3_bucket=s3_bucket,
                    s3_key=f'my_target/{project_name}/{project_name}_{mt_type_of_data}_{metric}_{etl_start_date}_{etl_end_date}.csv',
                    my_target_start_date=etl_start_date,
                    my_target_end_date=etl_end_date,
                    my_target_type_of_data=mt_type_of_data,
                    my_target_metric=metric,
                )
                save_data_to_s3_list.append(save_data_to_s3_)
            reduce(lambda x, y: [x >> y], save_data_to_s3_list)

        with TaskGroup(group_id='save_dimensions_data') as save_dimensions_data:
            save_dimensions_to_s3_list = []
            for dimension in mt_info_dimensions:
                save_dimensions_to_s3_ = MyTargetDimensionsToS3Operator(
                    task_id=f'save_dimensions_to_s3_{dimension}',
                    my_target_conn_id=mt_conn_id,
                    s3_conn_id=s3_conn_id,
                    s3_bucket=s3_bucket,
                    s3_key=f'my_target/{project_name}/{project_name}_{dimension}.csv',
                    my_target_start_date=etl_start_date,
                    my_target_end_date=etl_end_date,
                    my_target_type_of_data=mt_type_of_data,
                    my_target_dimension_name=dimension,
                )
                save_dimensions_to_s3_list.append(save_dimensions_to_s3_)
            reduce(lambda x, y: [x >> y], save_dimensions_to_s3_list)

        with TaskGroup(group_id='insert_metrics_from_s3') as insert_metrics_from_s3:
            insert_metrics_from_s3_list = []
            for metric in mt_metrics:
                insert_data_from_s3_ = ClickHouseOperator(
                    task_id=f'insert_data_from_s3_{metric}',
                    clickhouse_conn_id=ch_conn_id,
                    database=ch_database_name,
                    sql=sql_file_reader(project_name=project_name, query_filename=sql_query_metrics_filename,
                                        f_string=False),
                    params={'project_name': project_name,
                            'metric': metric,
                            's3_bucket': s3_bucket,
                            'mt_type_of_data': mt_type_of_data,
                            's3_key': BaseHook.get_connection(s3_conn_id).extra_dejson.get("aws_access_key_id"),
                            's3_secret': BaseHook.get_connection(s3_conn_id).extra_dejson.get(
                                "aws_secret_access_key")}
                )
                insert_metrics_from_s3_list.append(insert_data_from_s3_)
            reduce(lambda x, y: [x, y], insert_metrics_from_s3_list)

        with TaskGroup(group_id='insert_dimensions_from_s3') as insert_dimensions_from_s3:
            insert_dimensions_from_s3_list = []
            for dimension in mt_info_dimensions:
                insert_dimensions_from_s3_ = ClickHouseOperator(
                    task_id=f'insert_dimensions_from_s3_{dimension}',
                    clickhouse_conn_id=ch_conn_id,
                    database=ch_database_name,
                    sql=sql_file_reader(project_name=project_name, query_filename=sql_query_dimensions_filename,
                                        f_string=False),
                    params={'project_name': project_name,
                            'dimension': dimension,
                            's3_bucket': s3_bucket,
                            's3_key': BaseHook.get_connection(s3_conn_id).extra_dejson.get("aws_access_key_id"),
                            's3_secret': BaseHook.get_connection(s3_conn_id).extra_dejson.get(
                                "aws_secret_access_key")}
                )
                insert_dimensions_from_s3_list.append(insert_dimensions_from_s3_)
            reduce(lambda x, y: [x, y], insert_dimensions_from_s3_list)

    check_my_target_status_ >> create_tables >> drop_metrics_partitions >> truncate_info_dimensions >> save_metrics_data
    save_metrics_data >> save_dimensions_data >> insert_metrics_from_s3 >> insert_dimensions_from_s3

    return dag
