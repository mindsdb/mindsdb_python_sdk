import sys
import unittest
import os
import os.path
import time
import json
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
        cls.integrations = cls.sdk.integrations
        cls.cloud_sdk = SDK('https://cloud.mindsdb.com', user='george@cerebralab.com', password='12345678')
        cls.cloud_integrations = cls.cloud_sdk.integrations

        # need to have a uniq name for each launch to avoid race condition in cloud
        # mongo_darwin_python_3.8
        cls.integration_suffix = f"{sys.platform}_python{sys.version.split(' ')[0]}"
        cls.integration_creds = None
        with open(f'{os.path.expanduser("~")}/.mindsdb_credentials.json') as f:
            cls.integration_creds = json.load(f)

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

    def list_info(self, integrations):
        intg_arr = integrations.list_integrations()
        self.assertTrue(isinstance(intg_arr,list))

    def test_1_list_info_local(self):
        self.list_info(self.integrations)

    def test_1_list_info_cloud(self):
        self.list_info(self.cloud_integrations)


    def add_integration(self, _type, integrations):
        origin_name = f"{_type}_{self.integration_suffix}"
        try:
            del integrations[origin_name]
        except Exception as e:
            print(f"Attempting to delete {origin_name} has finished with {e}")

        integration_params = self.integration_creds[_type]
        integration_params["type"] = _type

        integrations[origin_name] = {"params": integration_params}
        self.assertTrue(isinstance(integrations[origin_name].get_info(), dict))
        self.assertTrue(len(integrations[origin_name]) > 5)

    def update_integration(self, _type, integrations, to_update=None):
        origin_name = f"{_type}_{self.integration_suffix}"
        integration = integrations[origin_name]
        self.assertTrue(integration is not None)
        update_params = self.integration_creds[_type]
        update_params["type"] = _type
        # update only one field
        # to make it private
        update_params["enabled"] = False
        if to_update is not None:
            update_params.update(to_update)

        integration.update({"params": update_params})

        self.assertTrue(not integration.get_info()["publish"])
        if to_update is not None:
            for k in to_update:
                self.assertTrue(integration.get_info()[k] == to_update[k])

    def test_2_add_clickhouse_local(self):
        self.add_integration("clickhouse", self.integrations)

    def test_2_clickhouse_cloud(self):
        self.add_integration("clickhouse", self.cloud_integrations)

    def test_3_update_clickhouse_local(self):
        self.update_integration("clickhouse", self.integrations)

    def test_3_update_clickhouse_cloud(self):
        self.update_integration("clickhouse", self.cloud_integrations)

    def test_4_add_mysql_local(self):
        self.add_integration("mysql", self.integrations)

    def test_4_add_myslq_cloud(self):
        self.add_integration("mysql", self.cloud_integrations)

    def test_5_update_mysql_local(self):
        self.update_integration("mysql", self.integrations)

    def test_5_update_mysql_cloud(self):
        self.update_integration("mysql", self.cloud_integrations)

    def test_6_add_mongo_local(self):
        self.add_integration("mongodb", self.integrations)

    def test_6_add_mongo_cloud(self):
        self.add_integration("mongodb", self.cloud_integrations)

    def test_7_update_mongo_local(self):
        self.update_integration("mongodb", self.integrations)

    def test_7_update_mongo_cloud(self):
        self.update_integration("mongodb", self.cloud_integrations)

    def test_8_add_mariadb_local(self):
        self.add_integration("mariadb", self.integrations)

    def test_8_add_mariadb_cloud(self):
        self.add_integration("mariadb", self.cloud_integrations)

    def test_9_update_mariadb_local(self):
        self.update_integration("mariadb", self.integrations)

    def test_9_update_mariadb_cloud(self):
        self.update_integration("mariadb", self.cloud_integrations)

    def test_10_add_postgres_local(self):
        self.add_integration("postgres", self.integrations)

    def test_10_add_postgres_cloud(self):
        self.add_integration("postgres", self.cloud_integrations)

    def test_11_update_postgres_local(self):
        self.update_integration("postgres", self.integrations)

    def test_11_update_postgres_cloud(self):
        self.update_integration("postgres", self.cloud_integrations)

    def test_12_add_snowflake_local(self):
        self.add_integration("snowflake", self.integrations)

    def test_12_add_snowflake_cloud(self):
        self.add_integration("snowflake", self.cloud_integrations)

    def test_13_update_snowflake_local(self):
        self.update_integration("snowflake", self.integrations, to_update={'test': True})

    def test_13_update_snowflake_cloud(self):
        self.update_integration("snowflake", self.cloud_integrations, to_update={'test': True})

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[-1] == "--no_backend_instance":
        # need to remove if from arg list
        # mustn't provide it into unittest.main
        sys.argv.pop()
        TestDatasources.start_backend = False
    unittest.main(verbosity=2)
