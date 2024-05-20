from openai import OpenAI


from mindsdb_sdk.utils.openai import (
                                      make_mindsdb_tool,
                                      execute_function_call,
                                      chat_completion_request,
                                      pretty_print_conversation)

import mindsdb_sdk
import os

from mindsdb_sdk.utils.table_schema import get_table_schemas

# generate the key at https://llm.mdb.ai
MINDSDB_API_KEY = os.environ.get("MINDSDB_API_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

MODEL = "gpt-3.5-turbo"


con = mindsdb_sdk.connect()

# given database name, returns schema and database object
# using example_db from mindsdb

# client_mindsdb_serve = OpenAI(
#     api_key=MINDSDB_API_KEY,
#     base_url="https://llm.mdb.ai"
# )

client_mindsdb_serve = OpenAI(
    api_key=OPENAI_API_KEY
)

database = con.databases.get("example_db")
schema = get_table_schemas(database, included_tables=["airline_passenger_satisfaction"])

tools = [make_mindsdb_tool(schema)]

SYSTEM_PROMPT = """You are a SQL expert. Given an input question, Answer user questions by generating SQL queries 
against the database schema provided in tools 
Unless the user specifies in the question a specific number of examples to obtain, query for at most 5 results using the 
LIMIT clause as per SQL standards. You can order the results to return the most informative data in the database.
Never query for all columns from a table. You must query only the columns that are needed to answer the question. 
Wrap each column name in backticks (`) to denote them as identifiers.
Pay attention to use only the column names you can see in the tables below. 
Be careful to not query for columns that do not exist. Also, pay attention to which column is in which table.
Pay attention to use CURRENT_DATE function to get the current date, if the question involves "today"."""

messages = [{
                "role":"system", "content":SYSTEM_PROMPT
            }, {"role":"user", "content":"what was the average delay on arrivals?"}]

chat_response = chat_completion_request(client=client_mindsdb_serve, model=MODEL, messages=messages, tools=tools, tool_choice=None)

assistant_message = chat_response.choices[0].message

assistant_message.content = str(assistant_message.tool_calls[0].function)

messages.append({"role": assistant_message.role, "content": assistant_message.content})

if assistant_message.tool_calls:
    results = execute_function_call(message=assistant_message, database=database)
    messages.append({
        "role": "function", "tool_call_id": assistant_message.tool_calls[0].id,
        "name": assistant_message.tool_calls[0].function.name,
        "content": results
    })

pretty_print_conversation(messages)
