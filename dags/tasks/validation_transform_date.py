"""Validation of new values"""
import logging
from io import StringIO
import pandas as pd
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine


def validation_transform_date(ti):
    """Validation before pushing"""
    logging.info("Starting validation of new values")
    df_json = ti.xcom_pull(key="transform_Dataset")
    df = pd.read_json(StringIO(df_json), orient="split")
    df_date = df[['date_id', 'year', 'month', 'day']]
    df_date = df_date.drop_duplicates(subset=['date_id'])
    conn = BaseHook.get_connection("postgres_database")
    engine = create_engine(f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}")

    table_name = 'tdate_db'
    primary_key = 'date_id'

    query = f"SELECT {primary_key} FROM {table_name}"
    df_existing = pd.read_sql(query, engine)
    df_existing[primary_key] = df_existing[primary_key].astype(int)

    new_values_date = df_date[~df_date[primary_key].isin(df_existing[primary_key])]
    new_values_date_json = new_values_date.to_json(orient="split")

    ti.xcom_push(key="new_values_date", value=new_values_date_json)
    logging.info("Finishing validation of new values of the table date")
    return new_values_date
