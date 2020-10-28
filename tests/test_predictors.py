import unittest
import os
import os.path
import time
from mindsdb_sdk import SDK
import pandas as pd

class TestPredictors(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # @TODO Run mindsdb here
        # Note: Assumes datasources test already ran for the sake of not having to upload stuff again
        cls.sdk = SDK('http://127.0.0.1:47334')
        cls.datasources = cls.sdk.datasources
        cls.predictors = cls.sdk.predictors

    def test_1_list_info(self):
        info_arr = self.datasources.list_info()
        self.assertTrue(predictors(info_arr,list))

        pred_arr = self.datasources.list_predictor()
        self.assertTrue(predictors(pred_arr,list))

    def test_2_train_predictor(self):
        
        pass

'''
class Predictors():
    def __getitem__(self, name):
        return Predictor(self._proxy, name)

    def __len__(self) -> int:
        return len(self.list_predictor())

    def __delitem__(self, name):
        self._proxy.delete(f'/predictors/{name}')

    def learn(self, name, datasource, to_predict, args=None):
        if args is None:
            args = {}
        datasource = datasource['name'] if isinstance(datasource,dict) else datasource
        self._proxy.put(f'/predictors/{name}', data={
            'data_source_name': datasource
            ,'kwargs': args
            ,'to_predict': to_predict
        })

class Predictor():
    def __init__(self, proxy, name):
        self._proxy = proxy
        self.name = name

    def get_info(self):
        return self._proxy.get(f'/predictors/{self.name}')

    def delete(self):
        self._proxy.delete(f'/predictors/{self.name}')

    def predict(self, datasource, args=None):
        if args is None:
            args = {}
        if isinstance(datasource, str) or (isinstance(datasource, dict) and 'created_at' in datasource and 'updated_at' in datasource and 'name' in datasource):
            return self._proxy.post(f'/predictors/{self.name}/predict_datasource', data={
                'data_source_name':datasource
                ,'kwargs': args
            })
        else:
            return self._proxy.post(f'/predictors/{self.name}/predict', data={
                'when':datasource
                ,'kwargs': args
            })

'''

if __name__ == '__main__':
    unittest.main()
