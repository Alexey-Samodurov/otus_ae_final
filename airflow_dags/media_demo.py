from datetime import datetime
from functools import reduce

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow_dbt import DbtRunOperator, DbtTestOperator


LOCAL_TZ = pendulum.timezone('Europe/Moscow')
DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': datetime(2023, 6, 6, tzinfo=LOCAL_TZ),
    'depends_on_past': False,
    'catchup': True
}
DBT_PROFILES_DIR = '/opt/airflow/'
DBT_DIR = '/opt/airflow/dbt_media/'
ETL_DATE = "{{ data_interval_end.strftime('%Y-%m-%d') }}"

with DAG(
        dag_id='media_demo',
        default_args=DEFAULT_ARGS,
        schedule_interval='@daily',
        max_active_runs=1,
        tags=['samodurov'],
) as dag:
    with TaskGroup('create_triggers') as create_triggers:
        triggers_list = []
        for dag_name in ['yandex_direct_custom_report_dinamic_factory',
                         'my_target_custom_report_dinamic_factory']:
            trigger_yd_ = TriggerDagRunOperator(
                task_id=f'trigger_dag_{dag_name}',
                trigger_dag_id=dag_name,
                execution_date=ETL_DATE,
                reset_dag_run=True,
                wait_for_completion=True,
            )
            triggers_list.append(trigger_yd_)
        reduce(lambda x, y: [x, y], triggers_list)

    dbt_build_stage_layer_ = DbtRunOperator(
        task_id='dbt_build_stage_layer',
        profiles_dir=DBT_PROFILES_DIR,
        dir=DBT_DIR,
        models='samodurov.stage'
    )

    dbt_build_marts_layer_ = DbtRunOperator(
        task_id='dbt_build_marts_layer',
        profiles_dir=DBT_PROFILES_DIR,
        dir=DBT_DIR,
        models='samodurov.marts'
    )

    dbt_run_marts_tests_ = DbtTestOperator(
        task_id='dbt_run_marts_tests',
        profiles_dir=DBT_PROFILES_DIR,
        dir=DBT_DIR,
        models='samodurov.marts'
    )


    def import_dashboard():
        from time import sleep
        from superset_connector.superset_connector import SupersetConnector

        sleep(60)
        ss = SupersetConnector(
            ss_url='http://superset:8088',
            username='admin',
            password='admin'
        )
        ss.import_dashboards(
            zip_config_path=f'{DBT_PROFILES_DIR}dashboard_export_20230625T103212.zip',
            passwords='{"databases/ClickHouse_Connect.yaml": ""}'
        )


    import_dashboard_ = PythonOperator(
        task_id='export_dashboard',
        python_callable=import_dashboard
    )

    create_triggers >> dbt_build_stage_layer_ >> dbt_build_marts_layer_ >> dbt_run_marts_tests_ >> import_dashboard_
