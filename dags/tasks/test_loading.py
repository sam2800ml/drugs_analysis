"""In this file is the uploading the data into the postgress sql"""
import logging

import pandas as pd
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine


def test_loading(ti):
    """
    This function loads a dataset into the 'drug_database' table in PostgreSQL.
    It retrieves the dataset from XCom, checks for its existence,
    and performs a bulk insert.
    If the dataset is not found or an error occurs during insertion,
    appropriate errors are raised.

    Args:
        ti (TaskInstance): Airflow TaskInstance object, used to pull data from XCom.
    """
    logging.info("Starting the loading to the database")
    csv = ti.xcom_pull(key='new_raw_values')
    if csv is None:
        raise ValueError("No CSV data found in XCom for key 'new_raw_values'")

    conn = BaseHook.get_connection("postgres_database")

    engine = create_engine(f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}")
    try:
        csv.to_sql('drug_database', engine, if_exists='append', index=False,
                method='multi')
        logging.info("Bulk insert completed successfully.")
    except Exception as e:
        logging.error("Error during bulk insert: %s", e)
        raise
