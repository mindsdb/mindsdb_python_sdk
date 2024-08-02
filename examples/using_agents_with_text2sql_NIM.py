import mindsdb_sdk
from uuid import uuid4
import os

con = mindsdb_sdk.connect('http://127.0.0.1:47334')

nim_api_key = os.getenv('NIM_API_KEY')
model_name = 'meta/llama-3_1-8b-instruct'
provider = 'nvidia_nim'

# Now create an agent that will use the model we just created.
agent = con.agents.create(name=f'mindsdb_sql_agent_llama-3.1-8b-instruct_{uuid4().hex}',
                          model=model_name,
                          provider=provider,
                          api_keys={provider: nim_api_key}
                          )

print('created agent')

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
answer = agent.completion([{'question': question, 'answer': None}])
print(answer.content)

con.databases.drop(database.name)
con.agents.drop(agent.name)
