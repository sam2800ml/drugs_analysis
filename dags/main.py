"""
This is the creation of a data flow using airflow
"""
# Local imports
from datetime import timedelta, datetime

# Third-party imports
import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.suite.transfers.local_to_drive import (
    LocalFilesystemToGoogleDriveOperator
)
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator
)
# Local imports
from tasks.load import load_dataset
from tasks.test_loading import test_loading
from tasks.Transform import transform_data
from tasks.create_table import create_table
from tasks.create_transformtable import create_transformdatabase
from tasks.validation_rawdata import validation_rawdataset
from tasks.validation_transform import validation_transform
from tasks.validation_transform_date import validation_transform_date
from tasks.datetransform_load import loadingdatetransform_data
from tasks.datatransform_load import loadingtransform_data
from tasks.loading_datadrive import export_and_save_to_csv

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
# This constansts has to go
CONFIG_PATH = "/opt/airflow/config/config.yaml"

with open(CONFIG_PATH, "r", encoding="utf-8") as file:
    config = yaml.safe_load(file)


dag = DAG(
    'Fetch_data_from_csv',
    default_args=default_args,
    description=(
        "This is a simple pipeline to be bale to load a file into a "
        "database using python"
        ),
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,  # Limits concurrent DAG runs
    concurrency=1
)

load_dataset_task = PythonOperator(
    task_id='load_dataset',
    python_callable=load_dataset,
    dag=dag,
)

create_table_task = PythonOperator(
    task_id="create_table",
    python_callable=create_table,
    dag=dag
)
validation_raw_dataset = PythonOperator(
    task_id='validation_raw_dataset',
    python_callable=validation_rawdataset,
    dag=dag,
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

validationtransform_database = PythonOperator(
    task_id='validation_transform_database',
    python_callable=validation_transform,
    dag=dag
)

validationtransformdate_database = PythonOperator(
    task_id='validation_transform_date_database',
    python_callable=validation_transform_date,
    dag=dag
)

loadtransformdate_db = PythonOperator(
    task_id='Loading_transformdate_data',
    python_callable=loadingdatetransform_data,
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

loading_database = PythonOperator(
        task_id='loading_database_to_drive',
        python_callable=export_and_save_to_csv,
        dag=dag
)

upload_task = LocalFilesystemToGoogleDriveOperator(
            task_id="upload_file_to_drive",
            gcp_conn_id=config["database"]["GCP_CONN_ID"],
            local_paths=[config["database"]["file_path1"], config["database"]["file_path2"]],
            drive_folder=config["database"]["DRIVE_FOLDER"]
        )


[load_dataset_task, create_table_task] >> validation_raw_dataset >> test_loading_task >> gx_validation_task 
gx_validation_task >> [transform_task, transform_database]
transform_database >> [validationtransform_database, validationtransformdate_database]
transform_task >> validationtransform_database  >> loadtransform_db  >> gx_validation_task_transform
transform_task >> validationtransformdate_database >>  loadtransformdate_db >> gx_validation_task_date

gx_validation_task_transform >> loading_database 
gx_validation_task_date >> loading_database

loading_database >> upload_task
