"""Date transform load"""
import os
import logging
from io import StringIO
import pandas as pd
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine


def loadingdatetransform_data(ti):
    """
    This function loads transformed data into PostgreSQL tables
    (`tdate_db` and `transformed_db`).
    It retrieves the transformed dataset from XCom, processes it,
    and inserts it into the database.
    Args:
        ti (TaskInstance): Airflow TaskInstance object,
        used to pull data from XCom.
    """
    # Get the transformed data from XCom
    logging.info("Loading transform data into tdate_db")
    df_json = ti.xcom_pull(key='new_values_date')
    df = pd.read_json(StringIO(df_json), orient='split')
    path = "/opt/airflow/data"

    # Connect to the database
    conn = BaseHook.get_connection("postgres_database")
    engine = create_engine(f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}")
    # Get existing primary keys
    table_name = 'tdate_db'
    # Check if there are new records to insert
    if not df.empty:
        logging.info("New Records to insert: %s", len(df))
        csv_path = os.path.join(path, "transformed_date_transform.csv")
        df.to_csv(csv_path, index=False)
        df.to_sql(table_name, engine, if_exists="append", index=False)
    else:
        logging.info("No new records to insert")
    logging.info("Finishing Loading transform data into tdate_db")
