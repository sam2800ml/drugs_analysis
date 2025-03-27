import os
import logging
import pandas as pd
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine


def export_and_save_to_csv():
    """Define the path where the CSV will be saved"""
    path = "/opt/airflow/data"
    try:
        os.makedirs(path, exist_ok=True)
        logging.info("Directory created at %s", os.path.abspath(path))
    except Exception as e:
        logging.error("Failed to create directory: %s", str(e))
        raise  # Re-raise exception after logging

    try:
        # Get PostgreSQL connection details
        conn = BaseHook.get_connection("postgres_database")
        engine = create_engine(
            f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        )

        # Define table names
        tables = ['transformed_db', 'tdate_db']

        # Export each table to CSV
        for table in tables:
            try:
                logging.info("Exporting table %s...", table)
                df = pd.read_sql_table(table, engine)
                csv_path = os.path.join(path, f"{table}.csv")
                df.to_csv(csv_path, index=False)
                logging.info("Successfully saved %s to %s", table, csv_path)

            except Exception as e:
                logging.error("Failed to export table %s: %s", table, str(e))
                continue  # Continue with next table if one fails

    except Exception as e:
        logging.error("Database operation failed: %s", str(e))
        raise
