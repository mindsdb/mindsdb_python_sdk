import logging

import mindsdb_sdk
from uuid import uuid4
import os

from mindsdb_sdk.utils.agents import MindsDBSQLStreamParser

con = mindsdb_sdk.connect()

open_ai_key = os.getenv('OPENAI_API_KEY')
model_name = 'gpt-4o'

# Now create an agent that will use the model we just created.
agent = con.agents.create(name=f'mindsdb_sql_agent_{model_name}_{uuid4().hex}',
                          model=model_name)

# Set up a Postgres data source with our new agent.
data_source = 'postgres'
connection_args = {
    "user": "demo_user",
    "password": "demo_password",
    "host": "samples.mindsdb.com",
    "port": "5432",
    "database": "demo",
    "schema": "demo_data"
}
description = 'mindsdb demo database'
database = con.databases.create(
    f'mindsdb_sql_agent_datasource_{uuid4().hex}',
    data_source,
    connection_args
)

# Actually connect the agent to the datasource.
agent.add_database(database.name, [], description)

question = 'How many three-bedroom houses were sold in 2008?'

completion_stream = agent.completion_stream([{'question': question, 'answer': None}])

#default logging level is set to INFO, we can change it to DEBUG to see more detailed logs and get full agent steps
mdb_parser = MindsDBSQLStreamParser()
full_response, sql_query = mdb_parser.process_stream(completion_stream)

con.databases.drop(database.name)
con.agents.drop(agent.name)
