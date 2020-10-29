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
        info_arr = self.predictors.list_info()
        self.assertTrue(isinstance(info_arr,list))

        pred_arr = self.predictors.list_predictor()
        self.assertTrue(isinstance(pred_arr,list))

    def test_2_train(self):
        try:
            del self.predictors['test_predictors_1']
        except Exception as e:
            print(e)
        self.predictors.learn('test_predictors_1', 'test_2_file_datasource', 'y', args={
            'stop_training_in_x_seconds': 30
        })
        time.sleep(3)
        pred = self.predictors['test_predictors_1']
        self.assertTrue('status' in pred.get_info())

    def test_3_predict(self):
        pred = self.predictors['test_predictors_1']
        while pred.get_info()['status'] != 'complete':
            print('Predictor not done trainig, status: ', pred.get_info()['status'])
            time.sleep(3)

        prediction = pred.predict(datasource={'theta3': 1})
        self.assertTrue('prediction' in prediction)
        self.assertTrue('prediction' in prediction)


if __name__ == '__main__':
    unittest.main()
