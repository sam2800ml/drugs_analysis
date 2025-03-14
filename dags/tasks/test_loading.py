import pandas as pd
from airflow.hooks.base import BaseHook
import logging
from sqlalchemy import create_engine, text
from io import StringIO

def test_loading(ti):
    """
    This function loads a dataset into the 'drug_database' table in PostgreSQL.
    It retrieves the dataset from XCom, checks for its existence, and performs a bulk insert.
    If the dataset is not found or an error occurs during insertion, appropriate errors are raised.

    Args:
        ti (TaskInstance): Airflow TaskInstance object, used to pull data from XCom.
    """

    csv = ti.xcom_pull(key='Dataset')
    if not csv:
        raise ValueError("Not any csv found")
    df = pd.read_json(StringIO(csv), orient='split')
    conn = BaseHook.get_connection("postgres_database")

    engine = create_engine(f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}")
    try:
        df.to_sql('drug_database', engine, if_exists='append', index=False, method='multi')
        logging.info("Bulk insert completed successfully.")
    except Exception as e:
        logging.error(f"Error during bulk insert: {e}")
        raise

