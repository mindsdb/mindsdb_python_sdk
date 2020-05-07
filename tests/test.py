import unittest
import os
import os.path
import time
import sys
sys.path.append(os.getcwd())
from mindsdb_client import MindsDB

SERVER = 'http://127.0.0.1:8090'
CREDENTIAL = {'email': 'test@test.ru', 'password': '123456'}
DS_PATH = 'tests/home_rentals.csv'

if os.path.isfile(DS_PATH) is False:
    import requests
    r = requests.get("https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv")
    with open(DS_PATH, 'wb') as f:
        f.write(r.content)

class TestAll(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mdb = MindsDB(server=SERVER, params=CREDENTIAL)
        if 'test_ds' in cls.mdb.datasources.names():
            cls.mdb.datasources['test_ds'].delete()
        if 'test_predictor' in cls.mdb.predictors.names():
            cls.mdb.predictors['test_predictor'].delete()

    def test_1_createDataSource(self):
        self.mdb.datasources.add('test_ds', path=DS_PATH)
        self.assertTrue('test_ds' in self.mdb.datasources.names())

    def test_2_learnPredictor(self):
        p = self.mdb.predictors.learn('test_predictor', 'test_ds', ['rental_price'])
        i = 0
        while p['status'] != 'complete' and i < 60:
            time.sleep(1)
            i += 1
        self.assertTrue(p['status'] == 'complete')

    def test_3_predict(self):
        r = self.mdb.predictors['test_predictor'].predict({'number_of_rooms': '2','number_of_bathrooms': '1', 'sqft': '1190'})
        self.assertIsInstance(r, list)
        self.assertTrue(len(r) == 1)
        self.assertIsInstance(r[0], dict)
        self.assertTrue('rental_price' in r[0])

    def test_4_deletePredictor(self):
        self.mdb.predictors['test_predictor'].delete()
        self.assertTrue('test_predictor' not in self.mdb.predictors.names())

    def test_5_deleteDataSource(self):
        self.mdb.datasources['test_ds'].delete()
        self.assertTrue('test_ds' not in self.mdb.datasources.names())

if __name__ == '__main__':
    unittest.main()
