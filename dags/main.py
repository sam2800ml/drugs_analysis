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

import os


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




def load_dataset(ti):
    """
    Calling the dataset, to be able to use it in csv
    """
    import pandas as pd

    api_url = "https://data.ct.gov/resource/rybz-nyjw.csv"
    api_limit = 1000
    offset = 0
    df = []

    while True:
        dataframe_url = f"{api_url}?$limit={api_limit}&$offset={offset}"
        df_chunk = pd.read_csv(dataframe_url)

        if df_chunk.empty:
            break
        df.append(df_chunk)
        offset += api_limit
        logging.info(f"Current offset {offset}")
    df_complete = pd.concat(df, ignore_index=True)
    df_json = df_complete.to_json(orient="split")

    ti.xcom_push(key="Dataset", value=df_json)




def test_loading(ti):
    csv = ti.xcom_pull(key='Dataset')
    if not csv:
        raise ValueError("Not any csv found")
    df = pd.read_json(csv, orient='split')


    

    conn = BaseHook.get_connection("postgres_database")

    engine = create_engine(f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}")
    try:
        df.to_sql('drug_database', engine, if_exists='append', index=False, method='multi')
        logging.info("Bulk insert completed successfully.")
    except Exception as e:
        logging.error(f"Error during bulk insert: {e}")
        raise




load_dataset_task = PythonOperator(
    task_id='load_dataset',
    python_callable=load_dataset,
    dag=dag,
)
create_table_task = SQLExecuteQueryOperator(
    task_id="create_table",
    conn_id="postgres_database",
    sql="""
        CREATE TABLE IF NOT EXISTS drug_database (
            id SERIAL PRIMARY KEY,
            date VARCHAR(100) NOT NULL,
            datetype VARCHAR(100),
            age INT,
            sex VARCHAR(10),
            race VARCHAR(100),
            ethnicity VARCHAR(100),
            residencecity VARCHAR(100),
            residencecounty VARCHAR(100),
            residencestate VARCHAR(100),
            injurycity VARCHAR(100),
            injurycounty VARCHAR(100),
            injurystate VARCHAR(100),
            injuryplace VARCHAR(100),
            descriptionofinjury VARCHAR(200),
            deathcity VARCHAR(100),
            deathcounty VARCHAR(100),
            death_state VARCHAR(100),
            location VARCHAR(100),
            locationifother VARCHAR(100),
            cod VARCHAR(300),
            mannerofdeath VARCHAR(100),
            othersignifican VARCHAR(150),
            heroin VARCHAR(100),
            heroin_dc VARCHAR(100),
            cocaine VARCHAR(100),
            fentanyl VARCHAR(100),
            fentanylanalogue VARCHAR(100),
            oxycodone VARCHAR(100),
            oxymorphone	VARCHAR(100),
            ethanol	VARCHAR(100),
            hydrocodone	VARCHAR(100),
            benzodiazepine VARCHAR(100),
            methadone VARCHAR(100),
            meth_amphetamine	VARCHAR(100),
            amphet VARCHAR(100),
            tramad VARCHAR(100),
            hydromorphone VARCHAR(100),
            morphine_notheroin VARCHAR(100),
            xylazine VARCHAR(100),
            gabapentin VARCHAR(100),
            opiatenos VARCHAR(100),
            heroin_morph_codeine VARCHAR(100),
            other_opioid VARCHAR(100),
            anyopioid VARCHAR(100),
            other VARCHAR(100),
            residencecitygeo VARCHAR(100),
            injurycitygeo VARCHAR(100),
            deathcitygeo VARCHAR(100)

        );
    """,
    dag=dag
)


test_loading_task = PythonOperator(
    task_id='test_loading',
    python_callable=test_loading,
    dag=dag,
)
load_dataset_task >> create_table_task >> test_loading_task