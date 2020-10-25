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
        pass

if __name__ == '__main__':
    unittest.main()
