from openai import OpenAI, OpenAIError
from mindsdb_sdk.utils.table_schema import get_table_schemas
from mindsdb_sdk.utils.openai import make_openai_tool
import mindsdb_sdk
import os

# generate the key at https://llm.mdb.ai
MINDSDB_API_KEY = os.environ.get("MINDSDB_API_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

MODEL = "gpt-3.5-turbo"
# text2sql prompt here (e.g. "What is the average satisfaction of passengers in the airline_passenger_satisfaction table?")
SYSTEM_PROMPT = """You are a SQL expert. Given an input question, first create a syntactically correct SQL query to run, 
then look at the results of the query and return the answer to the input question.
Unless the user specifies in the question a specific number of examples to obtain, query for at most 5 results using the 
LIMIT clause as per SQL standards. You can order the results to return the most informative data in the database.
Never query for all columns from a table. You must query only the columns that are needed to answer the question. 
Wrap each column name in backticks (`) to denote them as identifiers.
Pay attention to use only the column names you can see in the tables below. 
Be careful to not query for columns that do not exist. Also, pay attention to which column is in which table.
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


def generate_system_prompt(system_prompt, schema):
    prompt = {
        "role": "system",
        "content": system_prompt.format(schema=schema)
    }
    return prompt


def generate_user_prompt(query):
    prompt = {
        "role": "user",
        "content": query
    }
    return prompt


def extract_sql_query(result):
    # Split the result into lines
    lines = result.split('\n')

    # Initialize an empty string to hold the query
    query = ""

    # Initialize a flag to indicate whether we're currently reading the query
    reading_query = False

    # Iterate over the lines
    for line in lines:
        # If the line starts with "SQLQuery:", start reading the query
        if line.startswith("SQLQuery:"):
            query = line[len("SQLQuery:"):].strip()
            reading_query = True
        # If the line starts with "SQLResult:", stop reading the query
        elif line.startswith("SQLResult:"):
            break
        # If we're currently reading the query, append the line to the query
        elif reading_query:
            query += " " + line.strip()

    # If no line starts with "SQLQuery:", return None
    if query == "":
        return None

    return query


con = mindsdb_sdk.connect()

database = con.databases.get(name="example_db")
schema = get_table_schemas(database, included_tables=["airline_passenger_satisfaction"])

try:
    # client_mindsdb_serve = OpenAI(
    #     api_key=MINDSDB_API_KEY,
    #     base_url="https://llm.mdb.ai"
    # )

    client_mindsdb_serve = OpenAI(
        api_key=OPENAI_API_KEY
    )

    chat_completion_gpt = client_mindsdb_serve.chat.completions.create(
        messages=[
            generate_system_prompt(SYSTEM_PROMPT, schema),
            generate_user_prompt(PROMPT)
        ],
        model=MODEL
    )

    response = chat_completion_gpt.choices[0].message.content

    query = extract_sql_query(response)

    print(f"Generated SQL query: {query}")

except OpenAIError as e:
    raise OpenAIError(f"An error occurred with the MindsDB Serve API: {e}")

result = database.query(query).fetch()
print(result)
