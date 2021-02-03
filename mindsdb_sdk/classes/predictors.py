import time
import pandas as pd
from pandas.util import hash_pandas_object
from mindsdb_sdk.classes.datasources import DataSources, DataSource
from mindsdb_sdk.helpers.net_helpers import sending_attempts
from mindsdb_sdk.helpers.exceptions import PredictorException, DataSourceException


class Predictor():
    def __init__(self, proxy, name):
        self._proxy = proxy
        self.name = name
        self.known_datasources = {}

    @staticmethod
    def _data_hash(data):
        if not isinstance(data, pd.DataFrame):
            raise Exception(f"invalid data type: {type(data)}. pandas.DataFrame expected")

        return hash_pandas_object(data).sum()

    @sending_attempts(exception_type=PredictorException)
    def get_info(self):
        return self._proxy.get(f'/predictors/{self.name}')

    def delete(self):
        self._proxy.delete(f'/predictors/{self.name}')

    def wait_readiness(self):
        while self.get_info()['status'] != 'complete':
            time.sleep(3)

    def predict(self, when_data, args=None):
        self.get_info()
        if isinstance(when_data, dict):
            json = when_data
            url = f'/predictors/{self.name}/predict'

        else:
            ds = self._check_datasource(when_data)
            json = {'data_source_name': ds.name}
            url = f'/predictors/{self.name}/predict_datasource'

        self.wait_readiness()
        return self._proxy.post(url, json=json)

    def _check_datasource(self, df):
        df_hash = self._data_hash(df)
        name = f"datasource_{df_hash}"
        datasource = DataSource(self._proxy, name)
        try:
            datasource.get_info()
        except DataSourceException:
            datasources = DataSources(self._proxy)
            datasources[name] = {'df': df}
        return datasource

    def learn(self, to_predict, from_data, args=None):
        if args is None:
            args = {}
        ds = self._check_datasource(from_data)

        self._proxy.put(f'/predictors/{self.name}', json={
            'data_source_name': ds.name,
            'kwargs': args,
            'to_predict': to_predict
        })


class Predictors():
    def __init__(self, proxy):
        self._proxy = proxy

    @sending_attempts(exception_type=PredictorException)
    def list_info(self):
        return self._proxy.get('/predictors')

    def  list_predictor(self):
        return [Predictor(self._proxy, x['name']) for x in self.list_info()]

    def __getitem__(self, name):
        predictors = (x['name'] for x in self.list_info())
        return Predictor(self._proxy, name) if name in predictors else None

    def __len__(self) -> int:
        return len(self.list_predictor())

    def __delitem__(self, name):
        self._proxy.delete(f'/predictors/{name}')

    def learn(self, name, datasource, to_predict, args=None):
        """Not sure that it is needed here. But left it now."""
        if args is None:
            args = {}
        datasource = datasource['name'] if isinstance(datasource, dict) else datasource
        self._proxy.put(f'/predictors/{name}', json={
            'data_source_name': datasource,
            'kwargs': args,
            'to_predict': to_predict
        })

    def __call__(self, name, **kwargs):
        return Predictor(self._proxy, name)


    '''
    @TODO:
    * Add custom predictor
    * Fit custom predictor
    * Upload predictor
    * Download predictor
    * Rename predictor
    '''
