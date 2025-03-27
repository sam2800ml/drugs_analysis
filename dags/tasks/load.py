"""This file is used to create the extraction from an api to be able to load the files"""
import logging
import pandas as pd


def load_dataset(ti):
    """
    Calling the dataset from url to be able to use it
    we have to iterate because have a limit per call
    Args:
        ti (TaskInstance): Airflow TaskInstance object, used to push data to XCom.
    """
    api_url = "https://data.ct.gov/resource/rybz-nyjw.csv"
    api_limit = 1000
    offset = 0
    df = []

    while True:
        dataframe_url = f"{api_url}?$limit={api_limit}&$offset={offset}"
        df_chunk = pd.read_csv(dataframe_url)

        if df_chunk.empty:
            break
        df.append(df_chunk)
        offset += api_limit
        logging.info("Current offset %s", offset)
    df_complete = pd.concat(df, ignore_index=True)
    df_json = df_complete.to_json(orient="split")
    ti.xcom_push(key="Dataset", value=df_json)
    return df_complete
