"""
This is a server file example for mindsdb_python_sdk.
Run this file as a Python script to start the MindsDB server.
"""
import mindsdb_sdk #import the mindsdb_sdk package

server = mindsdb_sdk.connect()
server = mindsdb_sdk.connect('http://127.0.0.1:47334')

#  Input your MindsDB Cloud Credentials below to connect to MindsDB Cloud
server = mindsdb_sdk.connect(email='your_mindsdb_email', password='your_mindsdb_password')
server = mindsdb_sdk.connect('https://cloud.mindsdb.com', email='your_mindsdb_email', password='your_mindsdb_password') # Connect to MindsDB server in the cloud

"""
The following code is an example of how to use the MindsDB server. You may comment, uncomment, delete, modify or add to this code as you see fit.
"""

databases = server.list_databases()

database = databases[1]
query = database.query('select * from your_database.your_table')
print(query.fetch())
print(database)

project = server.list_projects()
print(project)

project = server.get_project('your_project_name')
models = project.list_models()

print(models)
model = models[0]
model = project.get_model('your_model_name')

print(model)