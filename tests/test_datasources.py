import sys
import unittest
import os
import os.path
import time
from subprocess import Popen
import pandas as pd
from mindsdb_sdk import SDK

class TestDatasources(unittest.TestCase):
    start_backend = True

    @classmethod
    def setUpClass(cls):
        if cls.start_backend:
            cls.sp = Popen(
                ['python', '-m', 'mindsdb', '--api', 'http'],
                close_fds=True
            )
            time.sleep(40)
        cls.sdk = SDK('http://localhost:47334')
        cls.datasources = cls.sdk.datasources
        cls.cloud_sdk = SDK('https://cloud.mindsdb.com', user='george@cerebralab.com', password='12345678')
        cls.cloud_datasources = cls.cloud_sdk.datasources

    @classmethod
    def tearDownClass(cls):
        if cls.start_backend:
            try:
                conns = psutil.net_connections()
                pid = [x.pid for x in conns if x.status == 'LISTEN' and x.laddr[1] == 47334 and x.pid is not None]
                if len(pid) > 0:
                    os.kill(pid[0], 9)
                cls.sp.kill()
            except Exception:
                pass
            time.sleep(40)

    def list_info(self, datasources):
        ds_arr = datasources.list_info()
        self.assertTrue(isinstance(ds_arr,list))

    def test_1_list_info_local(self):
        self.list_info(self.datasources)

    def test_1_list_info_cloud(self):
        self.list_info(self.cloud_datasources)

    def file_datasource(self, datasources):
        try:
            del datasources['test_2_file_datasource']
        except Exception as e:
            print(e)

        datasources['test_2_file_datasource'] = {'file': 'datasets/kin8nm.csv'}
        self.assertTrue(isinstance(datasources['test_2_file_datasource'].get_info(), dict))

        self.assertTrue(len(datasources['test_2_file_datasource']) > 10)


    def test_2_file_datasource_local(self):
        self.file_datasource(self.datasources)

    def test_2_file_datasource_cloud(self):
        self.file_datasource(self.cloud_datasources)

    def df_as_csv(self, datasources):
        try:
            del datasources['test_3_file_datasource']
        except Exception as e:
            print(e)

        df = pd.read_csv('datasets/us_health_insurance.csv')
        datasources['test_3_file_datasource'] = {'df': df}

        self.assertTrue(isinstance(datasources['test_3_file_datasource'].get_info(),dict))

        self.assertTrue(len(datasources['test_3_file_datasource']) > 10)

    def test_3_df_as_csv_local(self):
        self.df_as_csv(self.datasources)

    def test_3_df_as_csv_cloud(self):
        self.df_as_csv(self.cloud_datasources)

    def check_list(self, datasources):
        for name in ['test_2_file_datasource', 'test_3_file_datasource']:
            self.assertTrue(name in [x.name for x in datasources.list_datasources()])
            self.assertTrue([x['name'] for x in datasources.list_info()])

    def test_4_list_local(self):
        self.check_list(self.datasources)

    def test_4_list_cloud(self):
        self.check_list(self.cloud_datasources)

    def test_5_len_local(self):
        self.assertTrue(len(self.datasources) >= 2)

    def test_5_len_local(self):
        self.assertTrue(len(self.cloud_datasources) >= 2)


    def analisys(self, datasources):
        df = pd.DataFrame({
                'z1': [x for x in range(100,110)]
                ,'z2': [x*2 for x in range(100,110)]
            })
        datasources['test_remote_analisys'] = {'df': df}

        self.assertTrue(isinstance(datasources['test_remote_analisys'].get_info(),dict))

        self.assertTrue(len(datasources['test_remote_analisys']) > 10)

        remote_datasource = datasources['test_remote_analisys']
        self.assertTrue(remote_datasource is not None)

        statistical_analysis = remote_datasource.analysis
        assert len(statistical_analysis) > 8

    def test_6_analisys_local(self):
        self.analisys(self.datasources)

    def test_6_analisys_cloud(self):
        self.analisys(self.cloud_datasources)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[-1] == "--no_backend_instance":
        # need to remove if from arg list
        # mustn't provide it into unittest.main
        sys.argv.pop()
        TestDatasources.start_backend = False
    unittest.main(verbosity=2)
