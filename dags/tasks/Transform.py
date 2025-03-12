import pandas as pd
from datetime import datetime
from io import StringIO

manner_map = {
    'Accident' : "Accident",
    'Pending' : 'Pending',
    'Unknown' : "Unknown",
    'Natural' : "Natural",
    'Acciddent':"Accident"
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
    'Mnew London': 'New London',  # Fix typo
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


def creating_dateid(df,date_column):
    df['year'] = pd.to_datetime(df[date_column]).dt.strftime('%Y').astype(int)
    df['month'] = pd.to_datetime(df[date_column]).dt.strftime('%m').astype(int)
    df['day'] = pd.to_datetime(df[date_column]).dt.strftime('%d').astype(int)
    df['date_id'] = pd.to_datetime(df[date_column]).dt.strftime('%Y%m%d').astype(int)


    return df


def drug_columns(df):
    columns = ['heroin','heroin_dc','cocaine','fentanyl','fentanylanalogue','oxycodone','oxymorphone','ethanol','hydrocodone','benzodiazepine','methadone','meth_amphetamine','amphet','tramad','hydromorphone','morphine_notheroin','xylazine','gabapentin','opiatenos','heroin_morph_codeine','other_opioid','anyopioid']
    df[columns] = df[columns].applymap(lambda x: 1 if x == 'Y' else 0)
    df['drug_count'] = df[columns].sum(axis=1)
    return df


def standarize_categoricalcolumns(df):
    for i in df.select_dtypes(include=['object', 'string']).columns:
        df[i] = df[i].fillna('Unknown').str.strip().str.title()
    return df


def remapping(df):
    df['mannerofdeath'] = df['mannerofdeath'].replace(manner_map)
    df['location'] = df['location'].replace(location_mapping)
    df['injurystate'] = df['injurystate'].replace(injurystate_mapping)
    df['injurycounty'] = df['injurycounty'].replace(injurycounty_mapping)
    df['race'] = df['race'].replace(race_mapping)
    df['ethnicity'] = df['ethnicity'].replace(ethnicity_mapping)
    return df

def drop_column(df):
    df.drop(['date'],axis=1,inplace=True)
    return df

def transform_data(ti):
    df_json = ti.xcom_pull(key='Dataset')
    df = pd.read_json(StringIO(df_json), orient='split')

    df = standarize_categoricalcolumns(df)
    df = creating_dateid(df, 'date')  # Change 'date' if using another column
    df = drug_columns(df)
    df = remapping(df)
    df = drop_column(df)
    df.replace("Unknown", None, inplace=True)
    df_json_transformed = df.to_json(orient="split")

    ti.xcom_push(key="transform_Dataset", value=df_json_transformed)
    return df
