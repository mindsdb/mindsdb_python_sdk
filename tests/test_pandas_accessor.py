import pytest
import pandas as pd
from mindsdb_sdk import AutoML, auto_ml_config


@pytest.mark.usefixtures("mindsdb")
class TestAccessor:

    @staticmethod
    def flow_test_body(when=None):
        df = pd.DataFrame({
                'x1': [x for x in range(100)]
                ,'x2': [x*2 for x in range(100)]
                ,'y': [y*3 for y in range(100)]
            })

        # Train a model on the dataframe
        predictor_ref = df.auto_ml.learn('y')
        # Predict from the original dataframe
        predictions = df.auto_ml.predict()
        assert len(predictions) == len(df)

        test_df = pd.DataFrame({
                'x1': [x for x in range(100,110)]
                ,'x2': [x*2 for x in range(100,110)]
            })

        # Get (run) the analysis of test_df
        statistical_analysis = test_df.auto_ml.analysis
        assert len(statistical_analysis) > 8

        # Predict from the test dataframe
        kwargs = {'name': predictor_ref}
        if when:
            kwargs["when_data"] = when
        for pred in test_df.auto_ml.predict(**kwargs):
            assert 'y' in pred and pred['y'] is not None

    def test_1_native_flow(self):
        auto_ml_config(mode='native')
        self.flow_test_body()

    def test_2_cloud_flow(self):

        # disabled until https://github.com/mindsdb/mindsdb/issues/994 not fixed
        return
        # We can swtich to using the API, for example on localhost, like this:
        auto_ml_config(mode='api',
                       connection_info={'host': 'https://cloud.mindsdb.com',
                                        'user': 'george@cerebralab.com',
                                        'password':'12345678'})
        self.flow_test_body()
    def test_2_local_flow(self):

        # disabled until https://github.com/mindsdb/mindsdb/issues/994 not fixed
        return
        # We can swtich to using the API, for example on localhost, like this:
        auto_ml_config(mode='api', connection_info={
            'host': 'http://localhost:47334'
        })
        self.flow_test_body()

    def test_3_local_flow_with_when_condition(self):
        # disabled until https://github.com/mindsdb/mindsdb/issues/994 not fixed
        return
        auto_ml_config(mode='api', connection_info={
            'host': 'http://localhost:47334'
        })
        self.flow_test_body(when={"when": {"x1": 1000, "x2": 2000}})

    def test_3_cloud_flow_with_when_condition(self):
        # disabled until https://github.com/mindsdb/mindsdb/issues/994 not fixed
        return
        auto_ml_config(mode='api',
                       connection_info={'host': 'https://cloud.mindsdb.com',
                                        'user': 'george@cerebralab.com',
                                        'password':'12345678'})
        self.flow_test_body(when={"when": {"x1": 1000, "x2": 2000}})
