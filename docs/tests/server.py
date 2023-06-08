"""
This is an example file for the mindsdb_python_sdk. You may name this file whatever you want, but it must be a .py file. You may also modify this file as you see fit. To test the MindsDB server, run this file from the command line using the following command: python3 server.py
"""
import mindsdb_sdk #import the mindsdb_sdk package

server = mindsdb_sdk.connect()
server = mindsdb_sdk.connect('http://127.0.0.1:47334')

server = mindsdb_sdk.connect(email='your_mindsdb_email', password='your_mindsdb_password')
server = mindsdb_sdk.connect('https://cloud.mindsdb.com', email='your_mindsdb_email', password='your_mindsdb_password')

"""
The following queries are examples of how to use the MindsDB server. You may comment, uncomment, delete, modify or add to this code as you see fit.
"""

# databases = server.list_databases()

# database = databases[1]
# query = database.query('select * from your_database.your_table')
# print(query.fetch())
# print(database)

# project = server.list_projects()
# print(project)

# project = server.get_project('your_project_name')
# models = project.list_models()

# print(models)
# model = models[0]
# model = project.get_model('your_model_name')

# print(model)