import unittest
import os
import os.path
import time
from mindsdb_sdk import SDK
import pandas as pd

class TestAll(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # @TODO Run mindsdb here
        cls.sdk = SDK('http://127.0.0.1:47334')
        cls.datasources = cls.sdk.datasources

    def test_1_list_info(self):
        ds_arr = self.datasources.list_info()
        self.assertTrue(isinstance(ds_arr,list))

    def test_2_file_datasource(self):
        del self.datasources['test_2_file_datasource']

        self.datasources['test_2_file_datasource'] = {
            'file': 'datasets/kin8nm.csv'
        }
        self.assertTrue(isinstance(self.datasources['test_2_file_datasource'].get_info(),dict))

        self.assertTrue(len(self.datasources['test_2_file_datasource']) > 10)

    def test_3_df_as_csv(self):
        del self.datasources['test_3_file_datasource']

        df = pd.read_csv('datasets/us_health_insurance.csv')

        self.datasources['test_3_file_datasource'] = {
            'df': df
        }

        self.assertTrue(isinstance(self.datasources['test_3_file_datasource'].get_info(),dict))

        self.assertTrue(len(self.datasources['test_3_file_datasource']) > 10)

if __name__ == '__main__':
    unittest.main()
