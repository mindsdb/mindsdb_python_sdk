# Python MindsDB SDK
It enables you to connect to a MindsDB server from python using HTTP API.

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
server = mindsdb_sdk.connect('https://cloud.mindsdb.com', login='a@b.com', password='-')

# Connect to MindsDB Pro

server = mindsdb_sdk.connect('http://<YOUR_INSTANCE_IP>', login='a@b.com', password='-', is_managed=True)

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
      'btc_view',
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

More examples in [Google colab notebook](
https://colab.research.google.com/drive/1QouwAR3saFb9ffthrIs1LSH5COzyQa11#scrollTo=k6IbwsKRPQCR
)

## API documentation

Api documentation can be found in: 
https://mindsdb.github.io/mindsdb_python_sdk/


**Generating api docs:**

Locally:

```commandline
cd docs

pip install -r requirements.txt

make html
```


**Online documentation** is updated by pushing in `docs` branch



## How to test
`
It runs all tests for components 

```bash
env PYTHONPATH=./ pytest
```

## How to Connect From a Python File

Create a file in your python project's root directory to store the connection details:

`server.py` 

Add the connection arguments with **your MindsDB credentials** to `server.py`:

```python
import mindsdb_sdk

server = mindsdb_sdk.connect()
server = mindsdb_sdk.connect('http://127.0.0.1:47334')

server = mindsdb_sdk.connect(email='your_mindsdb_email', password='your_mindsdb_password')
server = mindsdb_sdk.connect('https://cloud.mindsdb.com', email='your_mindsdb_email', password='your_mindsdb_password')
```

Open your terminal and type:

`python server.py` 

### Testing the Connection

Add test queries to `server.py` with `print()` statements to confirm the connection:

```python
import mindsdb_sdk #import the mindsdb_sdk package

server = mindsdb_sdk.connect()
server = mindsdb_sdk.connect('http://127.0.0.1:47334')

#  Input your MindsDB Cloud Credentials below to connect to MindsDB Cloud
server = mindsdb_sdk.connect(email='your_mindsdb_email', password='your_mindsdb_password')
server = mindsdb_sdk.connect('https://cloud.mindsdb.com', email='your_mindsdb_email', password='your_mindsdb_password') # Connect to MindsDB server in the cloud

databases = server.list_databases()

database = databases[1] # Database type object

query = database.query('select * from files.test_data')
print(database)
```

To see a full example, checkout:
`server.py`