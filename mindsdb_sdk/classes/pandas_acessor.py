import os
import pandas as pd

from mindsdb_sdk.classes import proxy
from mindsdb_sdk.classes import predictors
from mindsdb_sdk.classes import datasources


def auto_ml_config(connection_info=None):

    os.environ['MINDSDB_PANDAS_AUTOML_HOST'] = connection_info['host']
    # not gracefully but as temporary solution
    # need to reset auth env for each instance with 'api' case
    # to prevent collision
    for env in ('MINDSDB_PANDAS_AUTOML_USER', 'MINDSDB_PANDAS_AUTOML_PASSWORD', 'MINDSDB_PANDAS_AUTOML_TOKEN'):
        try:
            del os.environ[env]
        except KeyError:
            pass

    if 'user' in connection_info:
        os.environ['MINDSDB_PANDAS_AUTOML_USER'] = connection_info['user']

    if 'password' in connection_info:
        os.environ['MINDSDB_PANDAS_AUTOML_PASSWORD'] = connection_info['password']

    if 'token' in connection_info:
        os.environ['MINDSDB_PANDAS_AUTOML_TOKEN'] = connection_info['token']



@pd.api.extensions.register_dataframe_accessor("auto_ml")
class AutoML:
    def __init__(self, pandas_obj):
        self._df = pandas_obj
        self._predictor = None
        self._analysis = None
        self._ds_name = str(pd.util.hash_pandas_object(self._df).sum())

        self.host = os.environ['MINDSDB_PANDAS_AUTOML_HOST']
        self.user = os.environ.get('MINDSDB_PANDAS_AUTOML_USER', None)
        self.password = os.environ.get('MINDSDB_PANDAS_AUTOML_PASSWORD', None)
        self.token = os.environ.get('MINDSDB_PANDAS_AUTOML_TOKEN', None)
        self.proxy = proxy.Proxy(self.host, user=self.user, password=self.password, token=self.token)
        self.datasource_controller = datasources.DataSources(self.proxy)
        self.predictor_controller = predictors.Predictors(self.proxy)


    @property
    def analysis(self):
        if self._analysis is not None:
            return self._analysis

        datasource = self.get_datatasource()
        self._analysis = datasource.analyze()

        return self._analysis

    def get_datatasource(self):
        # Upload if necessary

        datasource = self.datasource_controller[self._ds_name]
        if datasource is None:
            self.datasource_controller[self._ds_name] = {'df': self._df}
            datasource = self.datasource_controller[self._ds_name]
        return datasource


    def learn(self, to_predict, name=None, args=None, wait=True):
        self.get_datatasource()

        if name is None:
            name = self._ds_name
        if args is None:
            args = {}

        # recreate
        if self.predictor_controller[name] is not None:
            self.predictor_controller[name].delete()

        self._predictor = self.predictor_controller.learn(
            name,
            datasource=self._ds_name,
            to_predict=to_predict,
            wait=wait,
            **args
        )
        return name


    def predict(self, name=None, when_data=None):

        if name is not None:
            predictor = self.predictor_controller[name]
        else:
            predictor = self._predictor

        if when_data is None:
            when_data = self._df

        return predictor.predict(when_data=when_data)
