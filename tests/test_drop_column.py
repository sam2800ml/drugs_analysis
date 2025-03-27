""" Testinf the create_dateid"""
import sys
import os
import pandas as pd
import pytest


project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from dags.tasks.Transform import drop_column


def test_creating_dateid():
    """Testing the create_dateid function"""
    # Sample input DataFrame
    data = {'date': ['2012-05-29T00:00:00.000'], 'age': ['12'], "residencecitygeo": ["12.34532213"], "injurycitygeo": ["12.34532213"], "deathcitygeo": ["12.34532213"], "drug_column": [3]}
    df = pd.DataFrame(data)

    # Expected DataFrame after transformation
    expected_data = {
        "drug_column": [3]
    }
    expected_df = pd.DataFrame(expected_data)

    # Run the transformation
    result_df = drop_column(df.copy())

    # Use pandas testing utilities to compare DataFrames
    pd.testing.assert_frame_equal(result_df, expected_df)
