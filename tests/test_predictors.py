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
        cls.sdk = SDK('http://localhost:47334')
        cls.datasources = cls.sdk.datasources
        cls.predictors = cls.sdk.predictors

        if common.ENV in ('all', 'cloud'):
            cloud_host = common.CLOUD_HOST
            cloud_user, cloud_pass = common.generate_credentials(cloud_host)
            cls.cloud_sdk = SDK(cloud_host, user=cloud_user, password=cloud_pass)
            cls.cloud_datasources = cls.cloud_sdk.datasources
            cls.cloud_predictors = cls.cloud_sdk.predictors

        # need to have a uniq resource name for each launch to avoid race condition in cloud
        cls.datasource_test_2_name = f"test_2_file_datasource_{sys.platform}_python{sys.version.split(' ')[0]}"
        cls.predictor_test_1_name = f"test_predictor_1_{sys.platform}_python{sys.version.split(' ')[0]}"

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


    def list_info(self, predictors):
        info_arr = predictors.list_info()
        self.assertTrue(isinstance(info_arr,list))

        pred_arr = predictors.list_predictor()
        self.assertTrue(isinstance(pred_arr,list))

    def wait_predictor(self, predictors, predictor_name, waiting_limit=600):
        threshold = time.time() + waiting_limit

        while time.time() < threshold:
            pred = predictors[predictor_name]
            if pred is not None:
                break
        else:
            self.assertTrue(pred is not None,
                            f"could't access '{predictor_name}' in {waiting_limit} seconds")
        return pred

    def train(self, predictors):
        try:
            del predictors[self.predictor_test_1_name]
        except Exception as e:
            print(f"Attempting to delete {self.predictor_test_1_name} has finished with {e}")
        predictors.learn(self.predictor_test_1_name, self.datasource_test_2_name, 'y', args={
            'stop_training_in_x_seconds': 30
        })
        pred = self.wait_predictor(predictors, self.predictor_test_1_name)
        self.assertTrue('status' in pred.get_info())

    def predict(self, predictors):
        pred = self.wait_predictor(predictors, self.predictor_test_1_name)
        while pred.get_info()['status'] != 'complete':
            print('Predictor not done trainig, status: ', pred.get_info()['status'])
            time.sleep(3)

        pred_arr = pred.predict(when_data={'theta3': 1})
        self.assertTrue(len(pred_arr) == 1)
        self.assertTrue('y' in pred_arr[0])
        self.assertTrue(pred_arr[0]['y']['predicted_value'] is not None)

    @unittest.skipIf(common.ENV == 'cloud', "launched for cloud")
    def test_1_list_info_local(self):
        self.list_info(self.predictors)

    @unittest.skipIf(common.ENV == 'cloud', "launched for cloud")
    def test_2_train_local(self):
        self.train(self.predictors)

    @unittest.skipIf(common.ENV == 'cloud', "launched for cloud")
    def test_3_predict_local(self):
        self.predict(self.predictors)

    @unittest.skipIf(common.ENV == 'local', "launched for local")
    def test_1_list_info_cloud(self):
        self.list_info(self.cloud_predictors)

    @unittest.skipIf(common.ENV == 'local', "launched for local")
    def test_2_train_cloud(self):
        self.train(self.cloud_predictors)

    @unittest.skipIf(common.ENV == 'local', "launched for local")
    def test_3_predict_cloud(self):
        self.predict(self.cloud_predictors)

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[-1] == "--no_backend_instance":
        # need to remove if from arg list
        # mustn't provide it into unittest.main
        sys.argv.pop()
        TestPredictors.start_backend = False
    unittest.main()
