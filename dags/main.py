# posgress ipaddress 172.20.0.2
# to be able to acces the postgress sql we type on the terminal psql -h localhost -p 5432 -U airflow -d airflow
# airflow hook to be able to connect to postgresql database

from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
import logging
import pandas as pd
import yaml
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, text
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
from io import StringIO

from tasks.datatransform_load import loadingtransform_data
from tasks.load import load_dataset
from tasks.test_loading import test_loading
from tasks.Transform import transform_data
from tasks.create_table import create_table
from tasks.create_transformtable import create_transformdatabase


default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'start_date': datetime(2025,2,16),
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}

dag = DAG(
    'Fetch_data_from_csv',
    default_args=default_args,
    description="This is a simple pipeline to be bale to load a file into a database using python",
    schedule=timedelta(days=1)
)



load_dataset_task = PythonOperator(
    task_id='load_dataset',
    python_callable=load_dataset,
    dag=dag,
)
create_table_task = PythonOperator(
    task_id="create_table",
    python_callable=create_table,  # Use the imported function
    dag=dag
)

test_loading_task = PythonOperator(
    task_id='test_loading',
    python_callable=test_loading,
    dag=dag,
)

gx_validation_task = GreatExpectationsOperator(
    task_id="gx_validate_pg",
    conn_id="postgres_database",
    data_context_root_dir="/opt/airflow/gx",
    schema="public",
    data_asset_name="drug_database",
    expectation_suite_name="drugs_database_suite",
    return_json_dict=True,
    dag=dag
)
transform_task = PythonOperator(
    task_id='Transformation',
    python_callable=transform_data,
    dag=dag
)
transform_database = PythonOperator(
    task_id='creating_transform_database',
    python_callable=create_transformdatabase,
    dag=dag
)

loadtransform_db = PythonOperator(
    task_id='Loading_transform_data',
    python_callable=loadingtransform_data,
    dag=dag

)
gx_validation_task_transform = GreatExpectationsOperator(
    task_id="gx_validate_transform",
    conn_id="postgres_database",
    data_context_root_dir="/opt/airflow/gx",
    schema="public",
    data_asset_name="transformed_db",
    expectation_suite_name="expectations_tansform",
    return_json_dict=True,
    dag=dag
)
gx_validation_task_date = GreatExpectationsOperator(
    task_id="gx_validate_date",
    conn_id="postgres_database",
    data_context_root_dir="/opt/airflow/gx",
    schema="public",
    data_asset_name="tdate_db",
    expectation_suite_name="expectations_date",
    return_json_dict=True,
    dag=dag
)
[load_dataset_task, create_table_task] >> test_loading_task >> gx_validation_task >> transform_task >> transform_database >> loadtransform_db >> [gx_validation_task_transform,gx_validation_task_date]

