import time
from mindsdb_sdk.classes.proxy import Proxy
from mindsdb_sdk.classes.datasources import DataSources
from mindsdb_sdk.classes.predictors import Predictors
from mindsdb_sdk.classes.intergrations import Integrations
from mindsdb_sdk.classes.files import Files


class SDK():
    def __init__(self, host, user=None, password=None, token=None, url_token=None):
        self.proxy = Proxy(host, user, password, token, url_token)
        conn = False
        for _ in range(2):
            if self.proxy.ping():
                conn = True
            else:
                time.sleep(5)

        if not conn:
            raise Exception(f'Failed to connect to mindsdb host: {host} !')
        
        print(f'Connected to mindsdb host: {host} !')

        self.datasources = DataSources(self.proxy)
        self.predictors = Predictors(self.proxy)
        self.integrations = Integrations(self.proxy)
        self.files = Files(self.proxy)

    def ping(self):
        return self.proxy.ping()
