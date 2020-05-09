# Python MindsDB SDK
It enables you to connect to a midnsDB server and use it in a similar way to mindsb_native.

## Install
```
pip install mindsdb-client
```

## example of usage
```
from mindsdb_client import MindsDB

# connect
mdb = MindsDB(server='https://mindsdb.com', params={'email': 'test@email.com', 'password': 'secret'})

# upload datasource
mdb.datasources.add('rentals_ds', path='home_rentals.csv')

# create a new predictor and learn to predict
predictor = mdb.predictors.learn(
    name='home_rentals_price',
    data_source_name='rentals_ds',
    to_predict=['rental_price']
)

# predict
result = predictor.predict({'number_of_rooms': '2','number_of_bathrooms': '1', 'sqft': '1190'})
```

## tests

Before run tests, change SERVER and CREDENTIAL constants in `tests/test.py` to relevant. After run `python3 tests/test.py`  
Test file - is a place where you can find some examples of api usage.

## API Reference

### class MindsDB(server: str, params: dict)

### class DataSources()

### class DataSource()

### class Predictors()

### class Predictor()

### class Proxy()
