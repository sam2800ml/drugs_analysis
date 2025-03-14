import pandas as pd
from airflow.hooks.base import BaseHook
import logging
from sqlalchemy import create_engine, text
from io import StringIO

def validation_transform(ti):
    df_json = ti.xcom_pull(key="transform_Dataset")
    df = pd.read_json(StringIO(df_json),orient="split")
    
    df = df.drop(['year', 'month', 'day'], axis=1)
    logging.info(f"DataFrame columns: {df.columns.tolist()}")
    

    logging.info(f"DataFrame types:\n{df.dtypes}")



    conn = BaseHook.get_connection("postgres_database")
    engine = create_engine(f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}")

    table_name = 'transformed_db'
    primary_key = 'date_id'

    query = f"SELECT {primary_key} FROM {table_name}"
    df_existing = pd.read_sql(query,engine)
    df_existing[primary_key] = df_existing[primary_key].astype(int)

    logging.info(f"Existing DataFrame columns: {df_existing.columns.tolist()}")
    logging.info(f"Existing DataFrame types:\n{df_existing.dtypes}")

    new_values_transform = df[~df[primary_key].isin(df_existing[primary_key])]

    ti.xcom_push(key="new_values_transform", value=new_values_transform)





