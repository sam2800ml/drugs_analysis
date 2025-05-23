"""
    In this file is beign done the validation of the data in the
    sql database to be able to just load new data
"""
import logging
from io import StringIO
import pandas as pd
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine


def validation_transform(ti):
    """
        Validation of the data before pushing to the new database
    """
    logging.info("Startin validation")
    df_json = ti.xcom_pull(key="transform_Dataset")
    df = pd.read_json(StringIO(df_json), orient="split")
    df = df.drop(['year', 'month', 'day'], axis=1)
    logging.info("DataFrame columns: %s", df.columns.tolist())
    logging.info("DataFrame types:\n %s", df.dtypes)
    conn = BaseHook.get_connection("postgres_database")
    engine = create_engine(
        f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}")

    table_name = 'transformed_db'
    primary_key = 'date_id'

    query = f"SELECT {primary_key} FROM {table_name}"
    df_existing = pd.read_sql(query, engine)
    df_existing[primary_key] = df_existing[primary_key].astype(int)

    logging.info("Existing DataFramecolumns: %s", df_existing.columns.tolist())
    logging.info("Existing DataFrame types:\n %s", df_existing.dtypes)

    new_values_transform = df[~df[primary_key].isin(df_existing[primary_key])]

    ti.xcom_push(key="new_values_transform", value=new_values_transform)
    logging.info("All validation transformation data is completed")
    return new_values_transform
