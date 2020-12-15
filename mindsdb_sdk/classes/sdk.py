from mindsdb_sdk.classes.proxy import Proxy
from mindsdb_sdk.classes.datasources import DataSources
from mindsdb_sdk.classes.predictors import Predictors
from mindsdb_sdk.classes.intergrations import Integrations


class SDK():
    def __init__(self, host, user=None, password=None, token=None):
        self.proxy = Proxy(host, user, password, token)
        if self.proxy.ping():
            print(f'Connected to mindsdb host: {host} !')
        else:
            print(f'Failed to connect to mindsdb host: {host} !')

        self.datasources = DataSources(self.proxy)
        self.predictors = Predictors(self.proxy)
        self.integrations = Integrations(self.proxy)
