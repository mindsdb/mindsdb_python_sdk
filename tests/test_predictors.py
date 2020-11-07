import unittest
import os
import os.path
import time
from mindsdb_sdk import SDK
import pandas as pd
from subprocess import Popen


class TestPredictors(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sp = Popen(
            ['python', '-m', 'mindsdb', '--api', 'http'],
            close_fds=True
        )
        time.sleep(40)
        # Note: Assumes datasources test already ran for the sake of not having to upload stuff again
        cls.sdk = SDK('http://localhost:47334')
        cls.datasources = cls.sdk.datasources
        cls.predictors = cls.sdk.predictors


    @classmethod
    def tearDownClass(cls):
        try:
            conns = psutil.net_connections()
            pid = [x.pid for x in conns if x.status == 'LISTEN' and x.laddr[1] == 47334 and x.pid is not None]
            if len(pid) > 0:
                os.kill(pid[0], 9)
            cls.sp.kill()
        except Exception:
            pass
        time.sleep(40)


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
        pred = self.predictors['test_predictors_1']
        self.assertTrue('status' in pred.get_info())

    def test_3_predict(self):
        pred = self.predictors['test_predictors_1']
        while pred.get_info()['status'] != 'complete':
            print('Predictor not done trainig, status: ', pred.get_info()['status'])
            time.sleep(3)

        pred_arr = pred.predict(datasource={'theta3': 1})
        self.assertTrue(len(pred_arr) == 1)
        self.assertTrue('y' in pred_arr[0])
        self.assertTrue(pred_arr[0]['y']['predicted_value'] is not None)


if __name__ == '__main__':
    unittest.main()
