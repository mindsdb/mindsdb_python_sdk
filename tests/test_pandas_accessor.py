import sys
import os
import unittest
import time
from subprocess import Popen
import pandas as pd
import psutil
from mindsdb_sdk import auto_ml_config
import common

class TestAccessor(unittest.TestCase):
    start_backend = True

    @classmethod
    def setUpClass(cls):
        if common.ENV in ('cloud'):
            cls.cloud_host = common.CLOUD_HOST
            cls.cloud_user, cls.cloud_pass = common.generate_credentials(cls.cloud_host)
        if cls.start_backend:
            cls.sp = Popen(
                ['python3', '-m', 'mindsdb', '--api', 'http'],
                close_fds=True
            )
            time.sleep(40)


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

    def flow_test_body(self, when=None):
        df = pd.DataFrame({
                'x1': [x for x in range(100)]
                ,'x2': [x*2 for x in range(100)]
                ,'y': [y*3 for y in range(100)]
            })

        # Train a model on the dataframe
        predictor_ref = df.auto_ml.learn('y')
        # Predict from the original dataframe
        predictions = df.auto_ml.predict()
        assert len(predictions) > 0

        test_df = pd.DataFrame({
                'x1': list(range(100,110))
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

    def test_1_native(self):
        auto_ml_config(mode='native')
        self.flow_test_body()

    def test_2_local_http(self):
        if common.ENV != 'local':
            return
        auto_ml_config(mode='api', connection_info={
            'host': 'http://localhost:47334'
        })
        self.flow_test_body()

    def test_3_cloud_http(self):
        if common.ENV != 'cloud':
            return
        auto_ml_config(mode='api',
                       connection_info={'host': self.cloud_host,
                                        'user': self.cloud_user,
                                        'password': self.cloud_pass})
        self.flow_test_body()


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[-1] == "--no_backend_instance":
        # need to remove if from arg list
        # mustn't provide it into unittest.main
        sys.argv.pop()
        TestAccessor.start_backend = False
    unittest.main()
