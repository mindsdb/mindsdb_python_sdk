import mindsdb_sdk
from uuid import uuid4
import os

con = mindsdb_sdk.connect()

open_ai_key = os.environ['OPENAI_API_KEY']

# Now create an agent that will use the model we just created.
agent = con.agents.create(name=f'mindsdb_sql_agent_{uuid4().hex}',
                          params={'openai_api_key': open_ai_key})

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


print('Ask a question: ')
question = 'What is the average rental price of homes?'
answer = agent.completion([{'question': question, 'answer': None}])
print(answer.content)
