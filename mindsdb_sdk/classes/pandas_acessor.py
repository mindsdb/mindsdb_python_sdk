import os
import pandas as pd

from mindsdb_native import Predictor as NativePredictor
from mindsdb_sdk.classes import proxy
from mindsdb_sdk.classes import predictors


def auto_ml_config(mode='native', connection_info=None):
    if mode == 'native':
        os.environ['MINDSDB_PANDAS_AUTOML_MODE'] = 'native'
    elif mode == 'api':
        os.environ['MINDSDB_PANDAS_AUTOML_MODE'] = 'api'
        os.environ['MINDSDB_PANDAS_AUTOML_HOST'] = connection_info['host']

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
        if self.mode == 'api':
            self.host = os.environ['MINDSDB_PANDAS_AUTOML_HOST']
            self.user = os.environ.get('MINDSDB_PANDAS_AUTOML_USER', None)
            self.password = os.environ.get('MINDSDB_PANDAS_AUTOML_PASSWORD', None)
            self.token = os.environ.get('MINDSDB_PANDAS_AUTOML_TOKEN', None)
            self.proxy = proxy.Proxy(self.host, user=self.user, password=self.password, token=self.token)
        self.predictor_class = predictors.Predictors(self.proxy) if self.mode == 'api' else NativePredictor

    @property
    def analysis(self):
        if self.mode == 'native':
            from mindsdb_native.libs.controllers.functional import analyse_dataset
            if self._analysis is None:
                self._analysis = analyse_dataset(self._df)
            return self._analysis
        raise Exception('API mode not supported for this call yet!')

    def learn(self, to_predict, name=None):

        if name is None:
            name = str(pd.util.hash_pandas_object(self._df).sum())
        self._predictor = self.predictor_class(name)
        self._predictor.learn(from_data=self._df, to_predict=to_predict)


    def predict(self, name=None, when_data=None):

        if name is not None:
            predictor = self.predictor_class(name)
        else:
            predictor = self._predictor
        if when_data is None:
            return predictor.predict(when_data=self._df)
        return predictor.predict(when_data=when_data)
