""" Testinf the create_dateid"""
import sys
import os
import pandas as pd
import pytest


project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from dags.tasks.Transform import creating_dateid


def test_creating_dateid():
    """Testing the create_dateid function"""
    # Sample input DataFrame
    data = {'edate': ['2012-05-29T00:00:00.000', '2012-06-01T00:00:00.000']}
    df = pd.DataFrame(data)

    # Expected DataFrame after transformation
    expected_data = {
        'edate': ['2012-05-29T00:00:00.000', '2012-06-01T00:00:00.000'],
        'year': [2012, 2012],
        'month': [5, 6],
        'day': [29, 1],
        'date_id': [20120529, 20120601]
    }
    expected_df = pd.DataFrame(expected_data)

    # Run the transformation
    result_df = creating_dateid(df.copy(), 'edate')

    # Use pandas testing utilities to compare DataFrames
    pd.testing.assert_frame_equal(result_df, expected_df)
