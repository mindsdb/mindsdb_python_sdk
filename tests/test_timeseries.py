import sys
import os
import os.path
import time
import unittest
from subprocess import Popen
import psutil
from mindsdb_sdk import SDK

import common


class TestPredictors(unittest.TestCase):
    start_backend = True

    @classmethod
    def setUpClass(cls):
        if cls.start_backend:
            cls.sp = Popen(
                ['python', '-m', 'mindsdb', '--api', 'http'],
                close_fds=True
            )
            time.sleep(40)
            # Note: Assumes datasources test already ran for the sake of not having to upload stuff again
        if common.ENV in ('cloud'):
            cloud_host = common.CLOUD_HOST
            cloud_user, cloud_pass = common.generate_credentials(cloud_host)
            cls.sdk = SDK(cloud_host, user=cloud_user, password=cloud_pass)
            cls.datasources = cls.sdk.datasources
            cls.predictors = cls.sdk.predictors
        else:
            cls.sdk = SDK('http://localhost:47334')
            cls.datasources = cls.sdk.datasources
            cls.predictors = cls.sdk.predictors

    @classmethod
    def tearDownClass(cls):
        if cls.start_backend:
            try:
                conns = psutil.net_connections()
                pid = [x.pid for x in conns if x.status == 'LISTEN' and x.laddr[1] == 47334 and x.pid is not None]
                if len(pid) > 0:
                    os.kill(pid[0], 9)
                cls.sp.kill()
            except Exception:
                pass
            time.sleep(40)

    def test_1_train(self):
        try:
            del self.datasources['covid_data']
        except Exception as e:
            print(f"Attempting to delete covid_data has finished with {e}")

        self.datasources['covid_data'] = {'file': 'datasets/covid_ICU.csv'}

        try:
            del self.predictors['covid_predictor']
        except Exception as e:
            print(f"Attempting to delete {'covid_predictor'} has finished with {e}")

        self.predictors.learn('covid_predictor', 'covid_data', 'pnew_case', args={
            'stop_training_in_x_seconds': 30,
            'timeseries_settings': {
                'order_by': ['time'],
                'group_by': ['state'],
                'window': 5,
                'use_previous_target': False
            }
        })

        self.assertTrue('status' in self.predictors['covid_predictor'].get_info())

    def test_2_predict(self):
        pred_arr = self.predictors['covid_predictor'].predict(when_data={'time': '2020-07-26', 'state': 'AK'})
        print(pred_arr)
        self.assertTrue(len(pred_arr) == 1)
        self.assertTrue('pnew_case' in pred_arr[0])
        self.assertTrue(pred_arr[0]['pnew_case']['predicted_value'] is not None)

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[-1] == "--no_backend_instance":
        # need to remove if from arg list
        # mustn't provide it into unittest.main
        sys.argv.pop()
        TestPredictors.start_backend = False
    unittest.main()
