import pandas as pd
from airflow.hooks.base import BaseHook
import logging
from sqlalchemy import create_engine, text
from io import StringIO
from sqlalchemy import DDL

def loadingtransform_data(ti):
    df_json = ti.xcom_pull(key='transform_Dataset')
    df = pd.read_json(StringIO(df_json), orient='split')
    df_date = df[['date_id','year','month','day']]
    logging.info(df_date)
    df = df.drop(['year','month','day'],axis=1)

    conn = BaseHook.get_connection("postgres_database")
    engine = create_engine(f"postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}")
    try:
        df_date = df_date.drop_duplicates(subset=['date_id'])
        with engine.connect() as connection:
            connection.execute(DDL("DROP TABLE IF EXISTS tdate_db CASCADE"))
        df_date.to_sql('tdate_db',engine,if_exists='replace',index=False, method='multi')
        logging.info("Bulk insert completed successfully.")
    except Exception as e:
        logging.error(f"Error during bulk insert: {e}")
        raise
    try:
        df.to_sql('transformed_db',engine,if_exists='append',index=False, method='multi')
        logging.info("Bulk insert completed successfully.")
    except Exception as e:
        logging.error(f"Error during bulk insert: {e}")
        raise



