import os
import pandas as pd

from mindsdb_sdk.classes import proxy
from mindsdb_sdk.classes import predictors
from mindsdb_sdk.classes import datasources


def auto_ml_config(mode='native', connection_info=None):
    if mode == 'native':
        os.environ['MINDSDB_PANDAS_AUTOML_MODE'] = 'native'
    elif mode == 'api':
        os.environ['MINDSDB_PANDAS_AUTOML_MODE'] = 'api'
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
    else:
        raise Exception(f'Invalid mode: {mode} for the pandas auto_ml accessor!')


@pd.api.extensions.register_dataframe_accessor("auto_ml")
class AutoML:
    def __init__(self, pandas_obj):
        self._df = pandas_obj
        self._predictor = None
        self._analysis = None
        self.mode = os.environ['MINDSDB_PANDAS_AUTOML_MODE']
        self._raw_name = str(pd.util.hash_pandas_object(self._df).sum())

        if self.mode == 'api':
            self.host = os.environ['MINDSDB_PANDAS_AUTOML_HOST']
            self.user = os.environ.get('MINDSDB_PANDAS_AUTOML_USER', None)
            self.password = os.environ.get('MINDSDB_PANDAS_AUTOML_PASSWORD', None)
            self.token = os.environ.get('MINDSDB_PANDAS_AUTOML_TOKEN', None)
            self.proxy = proxy.Proxy(self.host, user=self.user, password=self.password, token=self.token)
            self.remote_datasource_controller = datasources.DataSources(self.proxy)
            self.predictor_class = predictors.Predictors(self.proxy)
        else:
            from mindsdb_native import Predictor as NativePredictor
            self.predictor_class = NativePredictor

    @property
    def analysis(self):
        if self._analysis is not None:
            return self._analysis
        if self.mode == 'native':
            from mindsdb_native.libs.controllers.functional import analyse_dataset
            self._analysis = analyse_dataset(self._df)
        else:
            datasource = self.remote_datasource_controller[self._raw_name]
            if datasource is None:
                self.remote_datasource_controller[self._raw_name] = {'df': self._df}
                datasource = self.remote_datasource_controller[self._raw_name]
            self._analysis = datasource.analyze()

        return self._analysis

    def learn(self, to_predict, name=None):

        if name is None:
            name = self._raw_name
        self._predictor = self.predictor_class(name)
        self._predictor.learn(from_data=self._df, to_predict=to_predict)

        return name


    def predict(self, name=None, when_data=None):

        if name is not None:
            predictor = self.predictor_class(name)
        else:
            predictor = self._predictor
        if when_data is None:
            return predictor.predict(when_data=self._df)
        return predictor.predict(when_data=when_data)
