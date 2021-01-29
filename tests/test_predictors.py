import sys
import os
import os.path
import time
import pytest
from mindsdb_sdk import SDK


@pytest.mark.usefixtures("mindsdb")
class TestPredictors:
    sdk = SDK('http://localhost:47334')
    cloud_sdk = SDK('https://cloud.mindsdb.com', user='george@cerebralab.com', password='12345678')
    datasources = sdk.datasources
    predictors = sdk.predictors
    cloud_datasources = cloud_sdk.datasources
    cloud_predictors = cloud_sdk.predictors

    # need to have a uniq resource name for each launch to avoid race condition in cloud
    datasource_test_2_name = f"test_2_file_datasource_{sys.platform}_python{sys.version.split(' ')[0]}_{id(int)}"
    predictor_test_1_name = f"test_predictor_1_{sys.platform}_python{sys.version.split(' ')[0]}_{id(int)}"

    @staticmethod
    def list_info(predictors):
        info_arr = predictors.list_info()
        assert isinstance(info_arr,list)

        pred_arr = predictors.list_predictor()
        assert isinstance(pred_arr,list)

    @pytest.mark.parametrize("location",
                             ["local", "cloud"])
    def test_1_list_info(self, location):
        predictors = self.predictors if location == 'local' else self.cloud_predictors
        self.list_info(predictors)

    def train(self, predictors):
        try:
            del predictors[self.predictor_test_1_name]
        except Exception as e:
            print(f"Attempting to delete {self.predictor_test_1_name} has finished with {e}")
        predictors.learn(self.predictor_test_1_name, self.datasource_test_2_name, 'y', args={
            'stop_training_in_x_seconds': 30
        })
        pred = predictors[self.predictor_test_1_name]
        assert  'status' in pred.get_info()

    @pytest.mark.parametrize("location",
                             ["local", "cloud"])
    def test_2_train(self, location):
        predictors = self.predictors if location == 'local' else self.cloud_predictors
        self.train(predictors)

    def predict(self, predictors):
        pred = predictors[self.predictor_test_1_name]
        while pred.get_info()['status'] != 'complete':
            print('Predictor not done trainig, status: ', pred.get_info()['status'])
            time.sleep(3)

        pred_arr = pred.predict(when_data={'theta3': 1})
        assert len(pred_arr) == 1
        assert 'y' in pred_arr[0]
        assert pred_arr[0]['y']['predicted_value'] is not None

    @pytest.mark.parametrize("location",
                             ["local", "cloud"])
    def test_3_predict_local(self, location):
        predictors = self.predictors if location == 'local' else self.cloud_predictors
        self.predict(predictors)
