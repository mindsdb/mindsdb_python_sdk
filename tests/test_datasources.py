import sys
import time
import pytest
import pandas as pd
from mindsdb_sdk import SDK


@pytest.mark.usefixtures("mindsdb")
class TestDatasources:
    sdk = SDK('http://localhost:47334')
    datasources = sdk.datasources
    cloud_sdk = SDK('https://cloud.mindsdb.com', user='george@cerebralab.com', password='12345678')
    cloud_datasources = cloud_sdk.datasources
    # need to have a uniq name for each launch to avoid race condition in cloud
    # test_2_file_datasource_darwin_python_3.8
    datasource_test_2_name = f"test_2_file_datasource_{sys.platform}_python{sys.version.split(' ')[0]}_{id(int)}"
    datasource_test_3_name = f"test_3_file_datasource_{sys.platform}_python{sys.version.split(' ')[0]}_{id(int)}"


    @staticmethod
    def list_info(datasources):
        ds_arr = datasources.list_info()
        assert isinstance(ds_arr, list)

    @pytest.mark.parametrize("location",
                             ["local", "cloud"])
    def test_1_list_info(self, location):
        datasources = self.datasources if location == 'local' else self.cloud_datasources
        self.list_info(datasources)

    def file_datasource(self, datasources):
        try:
            del datasources[self.datasource_test_2_name]
        except Exception as e:
            print(f"Attempting to delete {self.datasource_test_2_name} has finished with {e}")

        datasources[self.datasource_test_2_name] = {'file': 'datasets/kin8nm.csv'}
        time.sleep(0.5)
        ds = datasources[self.datasource_test_2_name]

        assert ds is not None and isinstance(ds.get_info(), dict)
        assert len(ds) > 10

    @pytest.mark.parametrize("location",
                             ["local", "cloud"])
    def test_2_file_datasource(self, location):
        datasources = self.datasources if location == 'local' else self.cloud_datasources
        self.file_datasource(datasources)

    def df_as_csv(self, datasources):
        try:
            del datasources[self.datasource_test_3_name]
        except Exception as e:
            print(f"Attempting to delete {self.datasource_test_3_name} has finished with {e}")

        df = pd.read_csv('datasets/us_health_insurance.csv')
        datasources[self.datasource_test_3_name] = {'df': df}

        assert isinstance(datasources[self.datasource_test_3_name].get_info(),dict)
        ds = datasources[self.datasource_test_3_name]
        assert ds is not None and len(ds) > 10

    @pytest.mark.parametrize("location",
                             ["local", "cloud"])
    def test_3_df_as_csv(self, location):
        datasources = self.datasources if location == 'local' else self.cloud_datasources
        self.df_as_csv(datasources)

    def check_list(self, datasources):
        for name in [self.datasource_test_2_name, self.datasource_test_3_name]:
            assert name in [x.name for x in datasources.list_datasources()]
            assert [x['name'] for x in datasources.list_info()]

    @pytest.mark.parametrize("location",
                             ["local", "cloud"])
    def test_4_list(self, location):
        datasources = self.datasources if location == 'local' else self.cloud_datasources
        self.check_list(datasources)

    @pytest.mark.parametrize("location",
                             ["local", "cloud"])
    def test_5_len(self, location):
        datasources = self.datasources if location == 'local' else self.cloud_datasources
        assert len(datasources) >= 2

    @staticmethod
    def analisys(datasources):
        # need to have a uniq name for each launch to avoid race condition in cloud
        datasource_name = f"test_remote_analisys_{sys.platform}_python{sys.version.split(' ')[0]}"
        try:
            del datasources[datasource_name]
        except Exception as e:
            print(f"Attempting to delete {datasource_name} has finished with {e}")

        df = pd.DataFrame({
                'z1': [range(100,110)]
                ,'z2': [x*2 for x in range(100,110)]
            })
        datasources[datasource_name] = {'df': df}
        time.sleep(0.5)

        ds = datasources[datasource_name]
        assert ds is not None and isinstance(ds.get_info(), dict)
        assert len(ds) > 10


    @pytest.mark.parametrize("location",
                             ["local", "cloud"])
    def test_6_analisys(self, location):
        datasources = self.datasources if location == 'local' else self.cloud_datasources
        self.analisys(datasources)
