# Python MindsDB SDK
It enables you to connect to a midnsDB server and use it in a similar way to mindsb_native.

## Install
```
pip install mindsdb_sdk
```

## Example 

Connect:
```python
import mindsdb_sdk

# Connect to local server 

server = mindsdb_sdk.connect()
server = mindsdb_sdk.connect('http://127.0.0.1:47334')

# Connect to cloud server

server = mindsdb_sdk.connect(email='a@b.com', password='-')
server = mindsdb_sdk.connect('https://cloud.mindsdb.com', email='a@b.com', password='-')
```

Base usage:
```python

# database
databases = server.list_databases()

database = databases[0] # Database type object

# sql query
query = database.query('select * from table1')
print(query.fetch())

# create table
table = database.create_table('table2', query)


# project
project = server.get_project('proj')

# sql query
query = project.query('select * from database.table join model1')

# create view
view = project.create_view(
      'view1',
       query=query
)

# get view
views = project.list_views()
view = views[0]
df = view.fetch()

# get model
models = project.list_models()
model = models[0]

# using model
result_df = model.predict(df)
result_df = model.predict(query)

# create model
model = project.create_model(
      'rentals_model',
      predict='price',
      query=query,
)

```

## API documentation

Generating:

```commandline
pip install sphinx

cd docs

make html
```

API documentation will be generated in docs/build/html

## How to test

It runs all tests for components 

```bash
env PYTHONPATH=./ pytest
```
