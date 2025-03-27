"""
    Creating the two transform tables to load the data in their
    perspective tables
"""
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook


def create_transformdatabase():
    """
    This function creates two tables in a PostgreSQL database:
    1. `tdate_db`: A table to store date-related information.
    2. `transformed_db`: A table to store transformed data
    with various attributes.

    """
    logging.info("Starting the creation the transform databases")
    hook = PostgresHook(postgres_conn_id="postgres_database")
    conn = hook.get_conn()
    cursor = conn.cursor()

    create_date_table = """
        CREATE TABLE IF NOT EXISTS tdate_db(
            date_id VARCHAR(8) PRIMARY KEY,
            Year INT NOT NULL,
            Month INT NOT NULL,
            Day INT NOT NULL
        )
    """

    create_transform_table = """
        CREATE TABLE IF NOT EXISTS transformed_db(
            id SERIAL PRIMARY KEY,
            datetype VARCHAR(100),
            sex VARCHAR(10),
            race VARCHAR(100),
            ethnicity VARCHAR(100),
            residencecity VARCHAR(100),
            residencecounty VARCHAR(100),
            residencestate VARCHAR(100),
            injurycity VARCHAR(100),
            injurycounty VARCHAR(100),
            injurystate VARCHAR(100),
            injuryplace VARCHAR(100),
            descriptionofinjury VARCHAR(200),
            deathcity VARCHAR(100),
            deathcounty VARCHAR(100),
            death_state VARCHAR(100),
            location VARCHAR(100),
            locationifother VARCHAR(100),
            cod VARCHAR(300),
            mannerofdeath VARCHAR(100),
            othersignifican VARCHAR(150),
            heroin VARCHAR(100),
            heroin_dc VARCHAR(100),
            cocaine VARCHAR(100),
            fentanyl VARCHAR(100),
            fentanylanalogue VARCHAR(100),
            oxycodone VARCHAR(100),
            oxymorphone VARCHAR(100),
            ethanol VARCHAR(100),
            hydrocodone VARCHAR(100),
            benzodiazepine VARCHAR(100),
            methadone VARCHAR(100),
            meth_amphetamine VARCHAR(100),
            amphet VARCHAR(100),
            tramad VARCHAR(100),
            hydromorphone VARCHAR(100),
            morphine_notheroin VARCHAR(100),
            xylazine VARCHAR(100),
            gabapentin VARCHAR(100),
            opiatenos VARCHAR(100),
            heroin_morph_codeine VARCHAR(100),
            other_opioid VARCHAR(100),
            anyopioid VARCHAR(100),
            other VARCHAR(100),
            date_id VARCHAR(8) REFERENCES tdate_db(date_id),
            drug_count INT,
            residencecitygeo_latitude DECIMAL(9,6),
            residencecitygeo_longitude DECIMAL(9,6),
            injurycitygeo_latitude DECIMAL(9,6),
            injurycitygeo_longitude DECIMAL(9,6),
            deathcitygeo_latitude DECIMAL(9,6),
            deathcitygeo_longitude DECIMAL(9,6),
            age_groups VARCHAR(50)
        );
    """

    try:
        cursor.execute(create_date_table)
        cursor.execute(create_transform_table)
        conn.commit()
        logging.info("Tables 'transformed_db,tdate_db' created or already exists")
    except Exception as e:
        conn.rollback()
        logging.error("Error creating table: %s", e)
        raise
    finally:
        cursor.close()
        conn.close()
