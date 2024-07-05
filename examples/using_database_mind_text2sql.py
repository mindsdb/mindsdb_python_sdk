from uuid import uuid4

from openai import OpenAI
from mindsdb_sdk.utils.mind import create_mind
import os


# Load MindsDB API key from environment variable. or set it here.
MINDSDB_API_KEY = os.getenv('MINDSDB_API_KEY')

# Set the model name for mind to use
model_name = 'gpt-4'

# Set the base URL for the MindsDB LiteLLM proxy.
base_url = 'https://ai.dev.mindsdb.com'


# Connect to MindsDB LiteLLM proxy.
client = OpenAI(
    api_key=MINDSDB_API_KEY,
    base_url=base_url
)

# create a database mind
mind = create_mind(
    name = f'my_house_data_mind_{uuid4().hex}',
    description= 'House Sales',
    base_url= base_url,
    api_key= MINDSDB_API_KEY,
    model= model_name,
    data_source_type='postgres',
    data_source_connection_args={
        'user': 'demo_user',
        'password': 'demo_password',
        'host': 'samples.mindsdb.com',
        'port': '5432',
        'database': 'demo',
        'schema': 'demo_data'
    }
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
