"""In this file is created the sql database where we are going to keep
    all the raw data preprocess
"""
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook


def create_table():
    """Create the `drug_database` table in PostgreSQL if it doesn't exist."""
    hook = PostgresHook(postgres_conn_id="postgres_database")
    conn = hook.get_conn()
    cursor = conn.cursor()

    create_table_sql = """
        CREATE TABLE IF NOT EXISTS drug_database (
            id SERIAL PRIMARY KEY,
            date VARCHAR(100) NOT NULL,
            datetype VARCHAR(100),
            age INT,
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
            residencecitygeo VARCHAR(100),
            injurycitygeo VARCHAR(100),
            deathcitygeo VARCHAR(100)
        );
    """

    try:
        cursor.execute(create_table_sql)
        conn.commit()
        logging.info("Table 'drug_database' created or already exists.")
    except Exception as e:
        conn.rollback()
        logging.error("Error creating table:%s", e)
        raise
    finally:
        cursor.close()
        conn.close()
