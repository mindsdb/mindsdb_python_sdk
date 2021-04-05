import sys
import unittest
import os
import os.path
import time
from subprocess import Popen
import psutil
import pandas as pd
from mindsdb_sdk import SDK

import common

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

        if common.ENV in ('all', 'cloud'):
            cloud_host = common.CLOUD_HOST
            cloud_user, cloud_pass = common.generate_credentials(cloud_host)
            cls.sdk = SDK(cloud_host, user=cloud_user, password=cloud_pass)
            cls.datasources = cls.sdk.datasources
        else:
            cls.sdk = SDK('http://localhost:47334')
            cls.datasources = cls.sdk.datasources
        # need to have a uniq name for each launch to avoid race condition in cloud
        # test_2_file_datasource_darwin_python_3.8
        cls.datasource_test_2_name = f"test_2_file_datasource_{sys.platform}_python{sys.version.split(' ')[0]}"
        cls.datasource_test_3_name = f"test_3_file_datasource_{sys.platform}_python{sys.version.split(' ')[0]}"

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

    def file_datasource(self, datasources):
        try:
            del datasources[self.datasource_test_2_name]
        except Exception as e:
            print(f"Attempting to delete {self.datasource_test_2_name} has finished with {e}")

        datasources[self.datasource_test_2_name] = {'file': 'datasets/kin8nm.csv'}

        self.assertTrue(isinstance(datasources[self.datasource_test_2_name].get_info(), dict))
        self.assertTrue(len(datasources[self.datasource_test_2_name]) > 10)

    def df_as_csv(self, datasources):
        try:
            del datasources[self.datasource_test_3_name]
        except Exception as e:
            print(f"Attempting to delete {self.datasource_test_3_name} has finished with {e}")

        df = pd.read_csv('datasets/us_health_insurance.csv')
        datasources[self.datasource_test_3_name] = {'df': df}

        self.assertTrue(isinstance(datasources[self.datasource_test_3_name].get_info(),dict))
        self.assertTrue(len(datasources[self.datasource_test_3_name]) > 10)

    def check_list(self, datasources):
        for name in [self.datasource_test_2_name, self.datasource_test_3_name]:
            self.assertTrue(name in [x.name for x in datasources.list_datasources()])
            self.assertTrue([x['name'] for x in datasources.list_info()])

    def analisys(self, datasources):
        # need to have a uniq name for each launch to avoid race condition in cloud
        datasource_name = f"test_remote_analisys_{sys.platform}_python{sys.version.split(' ')[0]}"
        try:
            del datasources[datasource_name]
        except Exception as e:
            print(f"Attempting to delete {datasource_name} has finished with {e}")

        df = pd.DataFrame({
                'z1': [x for x in range(100,110)]
                ,'z2': [x*2 for x in range(100,110)]
            })
        datasources[datasource_name] = {'df': df}

        self.assertTrue(isinstance(datasources[datasource_name].get_info(),dict))

        self.assertTrue(len(datasources[datasource_name]) > 10)

        remote_datasource = datasources[datasource_name]
        self.assertTrue(remote_datasource is not None)

        statistical_analysis = remote_datasource.analyze()
        assert len(statistical_analysis) > 8


    def test_0_ping(self):
        online = self.sdk.ping()
        self.assertTrue(online)


    def test_1_list_info(self):
        self.list_info(self.datasources)


    def test_2_file_datasource(self):
        self.file_datasource(self.datasources)


    def test_3_df_as_csv(self):
        self.df_as_csv(self.datasources)


    def test_4_list(self):
        self.check_list(self.datasources)


    def test_5_len(self):
        self.assertTrue(len(self.datasources) >= 2)


    def test_6_analisys(self):
        self.analisys(self.datasources)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[-1] == "--no_backend_instance":
        # need to remove if from arg list
        # mustn't provide it into unittest.main
        sys.argv.pop()
        TestDatasources.start_backend = False
    unittest.main()
