import os
import pandas as pd

def auto_ml_config(mode='native', connection_info=None):
    if mode == 'native':
        os.environ['MINDSDB_PANDAS_AUTOML_MODE'] = 'native'
    elif mode == 'api':
        os.environ['MINDSDB_PANDAS_AUTOML_MODE'] = 'api'
        os.environ['MINDSDB_PANDAS_AUTOML_HOST'] = connection_info['host']

        if 'user' in connection_info:
            os.environ['MINDSDB_PANDAS_AUTOML_USER'] = connection_info['user']

        if 'password' in connection_info:
            os.environ['MINDSDB_PANDAS_AUTOML_USER'] = connection_info['password']

        if 'token' in connection_info:
            os.environ['MINDSDB_PANDAS_AUTOML_USER'] = connection_info['token']
    else:
        raise Exeption(f'Invalid mode: {mode} for the pandas auto_ml accessor!')



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

    @property
    def analysis(self):
        if self.mode == 'native':
            from mindsdb_native.libs.controllers.functional import analyse_dataset
            if self._analysis is None:
                self._analysis = analyse_dataset(self._df)
            return self._analysis
        else:
            raise Exception('API mode not supported for this call yet!')

    def learn(self, to_predict, name=None):
        if self.mode == 'native':
            from mindsdb_native import Predictor

            if name is None:
                name = str(pd.util.hash_pandas_object(self._df).sum())

            self._predictor = Predictor(name)
            self._predictor.learn(from_data=self._df, to_predict=to_predict)

            return name
        else:
            raise Exception('API mode not supported for this call yet!')


    def predict(self, name=None):
        if self.mode == 'native':
            from mindsdb_native import Predictor
            
            if name is not None:
                predictor = Predictor(name)
            else:
                predictor = self._predictor
            return predictor.predict(when_data=self._df)
        else:
            raise Exception('API mode not supported for this call yet!')
