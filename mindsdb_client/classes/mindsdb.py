from mindsdb_client.classes.proxy import Proxy
from mindsdb_client.classes.data_sources import DataSources
from mindsdb_client.classes.predictors import Predictors

class MindsDB(object):
    datasources = None
    predictors = None
    _proxy = None

    def __init__(self, server: str, params: object):
        if isinstance(params, object) is False:
            raise Exception()
        if 'token' not in params and not ('email' in params and 'password' in params):
            raise Exception()
        if isinstance(server, str) is False:
            raise Exception()

        self._server = server

        if 'token' in params:
            self._apikey = params['token']
            self._proxy = Proxy(self._server, self._apikey)
        else:
            self._proxy = Proxy(self._server)
            self._proxy.login(email=params['email'], password=params['password'])

        self._proxy.ping()
        self.datasources = DataSources(self._proxy)
        self.predictors = Predictors(self._proxy)
