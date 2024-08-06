from uuid import uuid4

from openai import OpenAI
from mindsdb_sdk.utils.mind import create_mind, DatabaseConfig
import os


# Load MindsDB API key from environment variable. or set it here.
MINDSDB_API_KEY = os.getenv('MINDSDB_API_KEY')

# Set the base URL for the MindsDB LiteLLM proxy.
base_url = 'https://llm.mdb.ai'


# Connect to MindsDB LiteLLM proxy.
client = OpenAI(
    api_key=MINDSDB_API_KEY,
    base_url=base_url
)

# Create a Database Config.
pg_config = DatabaseConfig(
    description='House Sales',
    type='postgres',
    connection_args={
        'user': 'demo_user',
        'password': 'demo_password',
        'host': 'samples.mindsdb.com',
        'port': '5432',
        'database': 'demo',
        'schema': 'demo_data'
    },
    tables=['house_sales']
)

# create a database mind
mind = create_mind(
    base_url= base_url,
    api_key= MINDSDB_API_KEY,
    name = f'my_house_data_mind_{uuid4().hex}',
    data_source_configs=[pg_config]
)

# Actually pass in our tool to get a SQL completion.
completion = client.chat.completions.create(
  model=mind.name,
  messages=[
    {'role': 'user', 'content': 'How many 2 bedroom houses sold in 2008?'}
  ],
  stream=False
)

print(completion.choices[0].message.content)
