import logging
def load_dataset(ti):
    """
    Calling the dataset, to be able to use it in csv
    """
    import pandas as pd

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
        logging.info(f"Current offset {offset}")
    df_complete = pd.concat(df, ignore_index=True)
    df_json = df_complete.to_json(orient="split")

    ti.xcom_push(key="Dataset", value=df_json)