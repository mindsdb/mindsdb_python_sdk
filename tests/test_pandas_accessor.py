import unittest
from mindsdb_sdk import AutoML, auto_ml_config
import pandas as pd

class TestAccessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    def test_1_native_flow(self):
        auto_ml_config(mode='native')

        df = pd.DataFrame({
                'x1': [x for x in range(100)]
                ,'x2': [x*2 for x in range(100)]
                ,'y': [y*3 for y in range(100)]
            })

        # Train a model on the dataframe
        predictor_ref = df.automl.learn('y')
        # Predict from the original dataframe
        predictions = df.automl.predict()
        assert len(predictions) == len(df)

        test_df = pd.DataFrame({
                'x1': [x for x in range(100,110)]
                ,'x2': [x*2 for x in range(100,110)]
            })

        # Get (run) the analysis of test_df
        statistical_analysis = test_df.automl.analysis
        assert len(statistical_analysis) > 8

        # Predict from the test dataframe
        for pred in test_df.automl.predict(predictor_ref):
            assert 'y' in pred and pred['y'] is not None

    def test_2_local_server_flow(self):
        # @TOOD: Implement
        return

        # We can swtich to using the API, for example on localhost, like this:
        auto_ml_config(mode='api', connection_info={
            'host': 'http://localhost:47334'
        })

    def test_3_cloud_flow(self):
        # @TOOD: Implement
        return

        auto_ml_config(mode='api', connection_info={
            'host': 'cloud.mindsdb.com'
            ,'user': 'george.hosu@mindsdb.com'
            ,'password': 'my_secret password'
        })


if __name__ == '__main__':
    unittest.main()
