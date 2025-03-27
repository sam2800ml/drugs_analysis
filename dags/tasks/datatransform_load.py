""" loading transform"""
import logging
import os
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine


def loadingtransform_data(ti):
    """
    This function loads transformed data into PostgreSQL tables
    (`tdate_db` and `transformed_db`).
    It retrieves the transformed dataset from XCom,
    processes it, and inserts it into the database.

    Args:
        ti (TaskInstance): Airflow TaskInstance object,
        used to pull data from XCom.
    """
    path = "/opt/airflow/data"
    try:
        os.makedirs("/opt/airflow/data", exist_ok=True)
        logging.info("Directory created at %s", os.path.abspath('/opt/airflow/data'))
    except Exception as e:
        logging.error("Failed to create directory: %s", str(e))

    df = ti.xcom_pull(key='new_values_transform')

    if df is None or df.empty:
        logging.info("No new data to enter")
        return
    conn = BaseHook.get_connection("postgres_database")
    engine = create_engine(f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}")
    table_name = 'transformed_db'

    if not df.empty:
        logging.info("New Records inserted: %s", len(df))
        df.to_sql(table_name, engine, if_exists="append", index=False)
        try:
            csv_path = os.path.join(path, "transformed_data_transform.csv")
            logging.info("Attempting to save CSV to: %s", csv_path)
            df.to_csv(csv_path, index=False)
            logging.info("Successfully saved CSV to: %s", csv_path)
            # Verify the file exists after saving
            if os.path.exists(csv_path):
                logging.info("File exists at %s", csv_path)
            else:
                logging.error("File does not exist at %s after saving", csv_path)
        except Exception as e:
            logging.error("Failed to save CSV: %s", str(e))
            logging.info("CSV saved to %s", csv_path)
    else:
        logging.info("No new record to insert")
