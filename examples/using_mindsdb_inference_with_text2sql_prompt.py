from openai import OpenAI
from mindsdb_sdk.utils.openai import extract_sql_query, query_database, chat_completion_request, \
    pretty_print_conversation

import mindsdb_sdk
import os

from mindsdb_sdk.utils.table_schema import get_table_schemas

# generate the key at https://llm.mdb.ai
MINDSDB_API_KEY = os.environ.get("MINDSDB_API_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

MODEL = "gpt-3.5-turbo"

# the prompt should be a question that can be answered by the database
SYSTEM_PROMPT = """You are a SQL expert. Given an input question, first create a syntactically correct SQL query to run, then look at the results of the query and return the answer to the input question.
Unless the user specifies in the question a specific number of examples to obtain, query for at most 5 results using the LIMIT clause as per SQL standards. You can order the results to return the most informative data in the database.
Never query for all columns from a table. You must query only the columns that are needed to answer the question. Wrap each column name in backticks (`) to denote them as identifiers.
Pay attention to use only the column names you can see in the tables below. Be careful to not query for columns that do not exist. Also, pay attention to which column is in which table.
Pay attention to use CURRENT_DATE function to get the current date, if the question involves "today".

Use the following format:

Question: <Question here>
SQLQuery: <SQL Query to run>
SQLResult: <Result of the SQLQuery>
Answer: <Final answer here>

Only use the following tables:

{schema}
"""
PROMPT = "what was the average delay on arrivals?"


def generate_system_prompt(system_prompt: str, schema: dict) -> dict:
    prompt = {
        "role":"system",
        "content":system_prompt.format(schema=schema)
    }
    return prompt


def generate_user_prompt(query: str) -> dict:
    prompt = {
        "role":"user",
        "content":query
    }
    return prompt


con = mindsdb_sdk.connect()

# given database name, returns schema and database object
# using example_db from mindsdb

database = con.databases.get("example_db")
schema = get_table_schemas(database, included_tables=["airline_passenger_satisfaction"])

# client_mindsdb_serve = OpenAI(
#     api_key=MINDSDB_API_KEY,
#     base_url="https://llm.mdb.ai"
# )

client_mindsdb_serve = OpenAI(
    api_key=OPENAI_API_KEY
)

messages = [
    generate_system_prompt(SYSTEM_PROMPT, schema),
    generate_user_prompt(PROMPT)
]

chat_response = chat_completion_request(client=client_mindsdb_serve, model=MODEL, messages=messages, tools=None,
                                        tool_choice=None)

# extract the SQL query from the response
query = extract_sql_query(chat_response.choices[0].message.content)

result = query_database(database, query)

# generate the user prompt with the query result, this will be used to generate the final response
query = generate_user_prompt(f"Given this SQLResult: {str(result)} provide Answer: ")

# add the query to the messages list
messages.append(query)

# generate the final response
chat_completion_gpt = chat_completion_request(client=client_mindsdb_serve, model=MODEL, messages=messages, tools=None,
                                              tool_choice=None)
response = chat_completion_gpt.choices[0].message.content

pretty_print_conversation(messages)

