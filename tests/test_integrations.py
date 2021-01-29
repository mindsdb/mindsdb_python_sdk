import sys
import os
import os.path
import json
import pytest

from mindsdb_sdk import SDK


def get_integration_creds():
    _var_name = 'DATABASE_CREDENTIALS_STRINGIFIED_JSON'
    _var_value = os.getenv(_var_name)
    if _var_value is None:
        with open(os.path.join(os.path.expanduser("~"), '.mindsdb_credentials.json'), 'r') as fp:
            _var_value = fp.read()
    assert _var_value is not None, _var_name + ' ' + 'is not set'
    return json.loads(_var_value)


@pytest.mark.usefixtures("mindsdb")
class TestDatasources:
    sdk = SDK('http://localhost:47334')
    integrations = sdk.integrations
    cloud_sdk = SDK('https://cloud.mindsdb.com',
                    user='george@cerebralab.com',
                    password='12345678')
    cloud_integrations = cloud_sdk.integrations

    # need to have a uniq name for each launch to avoid race condition in cloud
    # mongo_darwin_python_3.8
    integration_suffix = f"{sys.platform}_python{sys.version.split(' ')[0]}_{id(int)}"
    integration_creds = get_integration_creds()

    @staticmethod
    def list_info(integrations):
        intg_arr = integrations.list_integrations()
        assert isinstance(intg_arr, list)

    @pytest.mark.parametrize("location",
                             ["local", "cloud"])
    def test_1_list_info(self, location):
        integrations = self.integrations if location == 'local' else self.cloud_integrations
        self.list_info(integrations)

    def add_integration(self, _type, integrations):
        origin_name = f"{_type}_{self.integration_suffix}"
        try:
            del integrations[origin_name]
        except Exception as e:
            print(f"Attempting to delete {origin_name} has finished with {e}")

        integration_params = self.integration_creds[_type]
        integration_params["type"] = _type

        integrations[origin_name] = {"params": integration_params}
        assert isinstance(integrations[origin_name].get_info(), dict)
        assert len(integrations[origin_name]) > 5

    def update_integration(self, _type, integrations, to_update=None):
        origin_name = f"{_type}_{self.integration_suffix}"
        integration = integrations[origin_name]
        assert integration is not None
        update_params = self.integration_creds[_type]
        update_params["type"] = _type
        # update only one field
        # to make it private
        update_params["enabled"] = False
        if to_update is not None:
            update_params.update(to_update)

        integration.update({"params": update_params})

        assert not integration.get_info()["publish"]
        if to_update is not None:
            for k in to_update:
                assert integration.get_info()[k] == to_update[k]

    @pytest.mark.parametrize("location",
                             ["local", "cloud"])
    def test_2_add_clickhouse(self, location):
        integrations = self.integrations if location == 'local' else self.cloud_integrations
        self.add_integration("clickhouse", integrations)

    # def test_3_update_clickhouse_local(self):
    #     self.update_integration("clickhouse", self.integrations)

    # def test_3_update_clickhouse_cloud(self):
    #     self.update_integration("clickhouse", self.cloud_integrations)

    @pytest.mark.parametrize("location",
                             ["local", "cloud"])
    def test_4_add_mysql(self, location):
        integrations = self.integrations if location == 'local' else self.cloud_integrations
        self.add_integration("mysql", integrations)

    # def test_5_update_mysql_local(self):
    #     self.update_integration("mysql", self.integrations)

    # def test_5_update_mysql_cloud(self):
    #     self.update_integration("mysql", self.cloud_integrations)

    @pytest.mark.parametrize("location",
                             ["local", "cloud"])
    def test_6_add_mongo(self, location):
        integrations = self.integrations if location == 'local' else self.cloud_integrations
        self.add_integration("mongodb", integrations)

    # def test_7_update_mongo_local(self):
    #     self.update_integration("mongodb", self.integrations)

    # def test_7_update_mongo_cloud(self):
    #     self.update_integration("mongodb", self.cloud_integrations)

    @pytest.mark.parametrize("location",
                             ["local", "cloud"])
    def test_8_add_mariadb_local(self, location):
        integrations = self.integrations if location == 'local' else self.cloud_integrations
        self.add_integration("mariadb", integrations)

    # def test_9_update_mariadb_local(self):
    #     self.update_integration("mariadb", self.integrations)

    # def test_9_update_mariadb_cloud(self):
    #     self.update_integration("mariadb", self.cloud_integrations)

    @pytest.mark.parametrize("location",
                             ["local", "cloud"])
    def test_10_add_postgres(self, location):
        integrations = self.integrations if location == 'local' else self.cloud_integrations
        self.add_integration("postgres", integrations)

    # def test_11_update_postgres_local(self):
    #     self.update_integration("postgres", self.integrations)

    # def test_11_update_postgres_cloud(self):
    #     self.update_integration("postgres", self.cloud_integrations)

    # def test_12_add_snowflake_local(self):
    #     self.add_integration("snowflake", self.integrations)

    # def test_12_add_snowflake_cloud(self):
    #     self.add_integration("snowflake", self.cloud_integrations)

    # def test_13_update_snowflake_local(self):
    #     self.update_integration("snowflake", self.integrations, to_update={'test': True})

    # def test_13_update_snowflake_cloud(self):
    #     self.update_integration("snowflake", self.cloud_integrations, to_update={'test': True})
