import pandas as pd
from airflow.hooks.base import BaseHook
import logging
from sqlalchemy import create_engine, text
from io import StringIO
from sqlalchemy import DDL

def loadingtransform_data(ti):
    """
    This function loads transformed data into PostgreSQL tables (`tdate_db` and `transformed_db`).
    It retrieves the transformed dataset from XCom, processes it, and inserts it into the database.

    Args:
        ti (TaskInstance): Airflow TaskInstance object, used to pull data from XCom.
    """

    df = ti.xcom_pull(key='new_values_transform')
    logging.info(df)
    if df is None or df.empty:
        logging.info("No new data to enter")
        return
    conn = BaseHook.get_connection("postgres_database")
    engine = create_engine(f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}")
    table_name = 'transformed_db'

    if not df.empty:
        logging.info(f"New Records inserted: {len(df)}")
        df.to_sql(table_name,engine,if_exists="append", index=False)
    else:
        logging.info("No new record to insert")