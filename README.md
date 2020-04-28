# mindsdb_python_sdk
It enables you to connect to a midnsDB server and use it in a similar way as if you were running it locally

```
from mindsdb_client import *

# connect
mdb = MindsDB(server_url=url, {'name': x, 'password': y})

# create a new predictor and learn to predict
mdb.Predictor(name='home_rentals_price').learn(
    to_predict='rental_price', # the column we want to learn to predict given all the data in the file
    from_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv" # the path to the file where we can learn from, (note: can be url)
)

# predict
result = mdb.Predictor(name='home_rentals_price').predict(when={'number_of_rooms': 2,'number_of_bathrooms':1, 'sqft': 1190})
```
