import unittest
import os
import os.path
import time
from mindsdb_sdk import SDK
import pandas as pd
from subprocess import Popen

class TestDatasources(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sp = Popen(
            ['python', '-m', 'mindsdb', '--api', 'http'],
            close_fds=True
        )
        time.sleep(40)
        cls.sdk = SDK('http://localhost:47334')
        cls.datasources = cls.sdk.datasources

    @classmethod
    def tearDownClass(cls):
        try:
            conns = psutil.net_connections()
            pid = [x.pid for x in conns if x.status == 'LISTEN' and x.laddr[1] == 47334 and x.pid is not None]
            if len(pid) > 0:
                os.kill(pid[0], 9)
            cls.sp.kill()
        except Exception:
            pass
        time.sleep(40)

    def test_1_list_info(self):
        ds_arr = self.datasources.list_info()
        self.assertTrue(isinstance(ds_arr,list))

    def test_2_file_datasource(self):
        try:
            del self.datasources['test_2_file_datasource']
        except Exception as e:
            print(e)

        self.datasources['test_2_file_datasource'] = {
            'file': 'datasets/kin8nm.csv'
        }
        self.assertTrue(isinstance(self.datasources['test_2_file_datasource'].get_info(),dict))

        self.assertTrue(len(self.datasources['test_2_file_datasource']) > 10)

    def test_3_df_as_csv(self):
        try:
            del self.datasources['test_3_file_datasource']
        except Exception as e:
            print(e)

        df = pd.read_csv('datasets/us_health_insurance.csv')
        self.datasources['test_3_file_datasource'] = {
            'df': df
        }

        self.assertTrue(isinstance(self.datasources['test_3_file_datasource'].get_info(),dict))

        self.assertTrue(len(self.datasources['test_3_file_datasource']) > 10)

    def test_4_list(self):
        for name in ['test_2_file_datasource', 'test_3_file_datasource']:
            self.assertTrue(name in [x.name for x in self.datasources.list_datasources()])
            self.assertTrue([x['name'] for x in self.datasources.list_info()])

    def test_5_len(self):
        self.assertTrue(len(self.datasources) >= 2)



if __name__ == '__main__':
    unittest.main()
