"""
    In this file is being done the validation of the raw data in the
    sql database before pushing new data
"""
import logging
from io import StringIO
import pandas as pd
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine


def validation_rawdataset(ti):
    """
        Validation of the data before pushing to the new database
    """
    logging.info("Startin validation raw dara")
    df_json = ti.xcom_pull(key="Dataset")
    df = pd.read_json(StringIO(df_json), orient="split")
    logging.info("DataFrame columns: %s", df.columns.tolist())
    logging.info("DataFrame types:\n %s", df.dtypes)
    conn = BaseHook.get_connection("postgres_database")
    engine = create_engine(
        f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}")

    table_name = 'drug_database'
    primary_key = 'datetype'

    query = f"SELECT {primary_key} FROM {table_name}"
    df_existing = pd.read_sql(query, engine)
    df_existing[primary_key] = df_existing[primary_key]

    logging.info("Existing DataFramecolumns: %s", df_existing.columns.tolist())
    logging.info("Existing DataFrame types:\n %s", df_existing.dtypes)

    new_raw_values = df[~df[primary_key].isin(df_existing[primary_key])]

    ti.xcom_push(key="new_raw_values", value=new_raw_values)
    logging.info("All validation transformation data is completed")
    return new_raw_values
