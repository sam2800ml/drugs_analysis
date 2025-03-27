"""Transformation beign used to be able to have the data structure better"""
from io import StringIO
import re
import logging
import pandas as pd


manner_map = {
    'Accident': "Accident",
    'Pending': 'Pending',
    'Unknown': "Unknown",
    'Natural': "Natural",
    'Acciddent': "Accident"
}
location_mapping = {
    'Residence': 'Residence',
    'Decedent’S Home': 'Residence',
    "Decedent'S Home": 'Residence',

    'Hospital': 'Hospital',
    'Hiospital': 'Hospital',  # Fix typo
    'Hospital - Inpatient': 'Hospital',
    'Hospital - Er/Outpatient': 'Hospital',
    'Hospital - Dead On Arrival': 'Hospital',

    'Convalescent Home': 'Nursing Home',
    'Nursing Home': 'Nursing Home',

    'Assisted Living': 'Assisted Living',
    'Shelter': 'Shelter',

    'Hospice': 'Hospice',
    'Hospice Facility': 'Hospice',

    'Other (Specify)': 'Other',
    'Other': 'Other',

    'Unknown': 'Unknown'
}

injurystate_mapping = {
    'Ct': 'CT',
    'Connecticut': 'CT',
    'Ma': 'MA',
    'Massachussets': 'MA',
    'Ny': 'NY',
    'Uk': 'UK',
    'Unknown': 'Unknown'
}
injurycounty_mapping = {
    'Hartford': 'Hartford',
    'New Haven': 'New Haven',
    'East Haven': 'New Haven',
    'Hamden': 'New Haven',
    'Waterbury': 'New Haven',
    'Middlesex': 'Middlesex',
    'Fairfield': 'Fairfield',
    'New London': 'New London',
    'Mnew London': 'New London',
    'Windham': 'Windham',
    'Litchfield': 'Litchfield',
    'Tolland': 'Tolland',
    'Putnam': 'Putnam',
    'Washington': 'Washington',
    'Westchester': 'Westchester',
    'Worcester': 'Worcester',
    'Suffolk': 'Suffolk',
    'Unknown': 'Unknown'
}


race_mapping = {
    'Black': 'Black or African American',
    'Black Or African American': 'Black or African American',
    'Black Or African American / American Indian Lenni Lenape': 'Black or African American',
    'White': 'White',
    'Asian': 'Asian',
    'Asian Indian': 'Asian',
    'Asian/Indian': 'Asian',
    'Asian, Other': 'Asian',
    'Other Asian': 'Asian',
    'Chinese': 'Asian',
    'Korean': 'Asian',
    'American Indian Or Alaska Native': 'American Indian or Alaska Native',
    'Hawaiian': 'Native Hawaiian or Other Pacific Islander',
    'Other': 'Other',
    'Other (Specify)': 'Other',
    'Other (Specify) Haitian': 'Other',
    'Other (Specify) Portugese, Cape Verdean': 'Other',
    'Other (Specify) Puerto Rican': 'Other',
    'Native American, Other': 'Other',
    'Unknown': 'Unknown'
}
ethnicity_mapping = {
    'Hispanic': 'Hispanic or Latino',
    'Spanish/Hispanic/Latino': 'Hispanic or Latino',
    'Yes, Other Spanish/Hispanic/Latino': 'Hispanic or Latino',
    'Yes, Other Spanish/Hispanic/Latino (Specify)': 'Hispanic or Latino',
    'Puerto Rican': 'Puerto Rican',
    'Yes, Puerto Rican': 'Puerto Rican',
    'Mexican, Mexican American, Chicano': 'Mexican, Mexican American, Chicano',
    'Yes, Mexican, Mexican American, Chicano': 'Mexican, Mexican American, Chicano',
    'Cuban': 'Cuban',
    'Not Spanish/Hispanic/Latino': 'Not Hispanic or Latino',
    'No, Not Spanish/Hispanic/Latino': 'Not Hispanic or Latino',
    'Other Spanish/Hispanic/Latino': 'Other/Unknown',
    'Unknown': 'Other/Unknown'
}


def creating_dateid(df, date_column):
    """
    Creates a 'date_id' column and extracts year, month, and day from a date column.
    Args:
        df (pd.DataFrame): Input DataFrame.
        date_column (str): Name of the column containing date values.
    Returns:
        pd.DataFrame: DataFrame with added 'year', 'month', 'day', and 'date_id' columns.
    """
    logging.info("Starting creating the date_id")
    df['year'] = pd.to_datetime(df[date_column]).dt.strftime('%Y').astype(int)
    df['month'] = pd.to_datetime(df[date_column]).dt.strftime('%m').astype(int)
    df['day'] = pd.to_datetime(df[date_column]).dt.strftime('%d').astype(int)
    df['date_id'] = pd.to_datetime(df[date_column]).dt.strftime('%Y%m%d').astype(int)
    logging.info("finish creating the date_id")
    return df


def drug_columns(df):
    """
    Converts drug-related columns to binary (0 or 1) and calculates the total drug count.
    Args:
        df (pd.DataFrame): Input DataFrame.
    Returns:
        pd.DataFrame: DataFrame with binary drug columns and a 'drug_count' column.
    """
    logging.info("Starting standarizing the drug columns")
    columns = ['heroin', 'heroin_dc', 'cocaine', 'fentanyl',
               'fentanylanalogue', 'oxycodone', 'oxymorphone', 'ethanol',
               'hydrocodone', 'benzodiazepine', 'methadone',
               'meth_amphetamine', 'amphet', 'tramad', 'hydromorphone',
               'morphine_notheroin', 'xylazine', 'gabapentin', 'opiatenos',
               'heroin_morph_codeine', 'other_opioid', 'anyopioid']
    df[columns] = df[columns].applymap(lambda x: 1 if x == 'Y' else 0)
    df['drug_count'] = df[columns].sum(axis=1)
    logging.info("Finishing standarizing the drug columns")
    return df


def standarize_categoricalcolumns(df):
    """
    Standardizes categorical columns by filling missing values and formatting strings.
    Args:
        df (pd.DataFrame): Input DataFrame.
    Returns:
        pd.DataFrame: DataFrame with standardized categorical columns.
    """
    logging.info("Starting standarizing categorical columns")
    for i in df.select_dtypes(include=['object', 'string']).columns:
        df[i] = df[i].fillna('Unknown').str.strip().str.title()
    logging.info("Finishing standarizing categorical columns")
    return df


def remapping(df):
    """
    Replaces categorical values in specific columns using predefined
    mapping dictionaries.
    Args:
        df (pd.DataFrame): Input DataFrame.
    Returns:
        pd.DataFrame: DataFrame with remapped categorical values.
    """
    df['mannerofdeath'] = df['mannerofdeath'].replace(manner_map)
    df['location'] = df['location'].replace(location_mapping)
    df['injurystate'] = df['injurystate'].replace(injurystate_mapping)
    df['injurycounty'] = df['injurycounty'].replace(injurycounty_mapping)
    df['race'] = df['race'].replace(race_mapping)
    df['ethnicity'] = df['ethnicity'].replace(ethnicity_mapping)
    return df


def extract_coordinates(df, columns):
    """
    Extracts latitude and longitude from specified columns in a DataFrame.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the data.
    columns (list of str): List of column names to process.

    Returns:
    pd.DataFrame: DataFrame with new columns for latitude and longitude.
    """
    # Regular expression pattern to match coordinates
    coord_pattern = re.compile(r'\((-?\d+\.\d+),\s*(-?\d+\.\d+)\)')

    for col in columns:
        # Generate new column names for latitude and longitude
        lat_col = f'{col}_latitude'
        lon_col = f'{col}_longitude'

        # Extract coordinates using the regular expression
        coords = df[col].str.extract(coord_pattern)

        # Assign extracted coordinates to new columns and convert to float
        df[lat_col] = coords[0].astype(float)
        df[lon_col] = coords[1].astype(float)
    return df


def age_groups(df):
    """
    Put in the age in groups to be able to analyze them better
    """
    bins = [0, 18, 30, 46, 60, float('inf')]
    labels = ['0-18,Menores', '19-30,Jóvenes Adultos', '31-45,Adultos Jóvenes', '46-60,Adultos de Mediana Edad', '61+,Adultos Mayores']
    df["age_groups"] = pd.cut(df["age"], bins=bins, labels=labels, right=True) # right=True it means that the right number is include
    return df


def drop_column(df):
    """
    Drops the 'date' column from the DataFrame.
    Args:
        df (pd.DataFrame): Input DataFrame.
    Returns:
        pd.DataFrame: DataFrame without the 'date' column.
    """
    logging.info("Starting dropping date column")
    df.drop(['date', 'age', 'residencecitygeo', 'injurycitygeo', 'deathcitygeo'], axis=1, inplace=True)
    logging.info("Finishing dropping date column")
    return df


def transform_data(ti):
    """
    Transforms the dataset by standardizing, remapping, and cleaning data.
    Args:
        ti (TaskInstance): Airflow TaskInstance object to pull and push data via XCom.
    Returns:
        pd.DataFrame: Transformed DataFrame.
    """
    logging.info("Starting all the transformations")
    df_json = ti.xcom_pull(key='Dataset')
    df = pd.read_json(StringIO(df_json), orient='split')
    df = standarize_categoricalcolumns(df)
    df = creating_dateid(df, 'date')  # Change 'date' if using another column
    df = drug_columns(df)
    df = remapping(df)
    columns_to_process = ['residencecitygeo', 'injurycitygeo', 'deathcitygeo']
    df = extract_coordinates(df, columns_to_process)
    df = age_groups(df)
    df = drop_column(df)
    df.replace("Unknown", None, inplace=True)
    df_json_transformed = df.to_json(orient="split")
    logging.info("All transformations are completed, ready to push to database")
    ti.xcom_push(key="transform_Dataset", value=df_json_transformed)
    return df
