# Python MindsDB SDK
It enables you to connect to a midnsDB server and use it in a similar way to mindsb_native.

## Install
```
pip install mindsdb_sdk
```

## Example of usage
```python
from mindsdb_sdk import SDK

# connect
mdb = SDK('http://localhost:47334')

# upload datasource
mdb.datasources['home_rentals_data'] = {'file' : 'home_rentals.csv'}

# create a new predictor and learn to predict
predictor = mdb.predictors.learn(
    name='home_rentals',
    datasource='home_rentals_data',
    to_predict='rental_price'
)

# predict
result = predictor.predict({'initial_price': '2000','number_of_bathrooms': '1', 'sqft': '700'})
```

## Tests

Before run tests, change SERVER and CREDENTIAL constants in `tests/test.py`. After that, run `python3 tests/test.py`  
Test file - is a good place where you can find some examples of api usage.

## API Reference(WIP)

### class MindsDB(server: str, params: dict)

### class DataSources()

### class DataSource()

### class Predictors()

### class Predictor()

### class Proxy()
