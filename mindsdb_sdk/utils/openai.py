import json
from logging import getLogger
from typing import List

import openai
from openai.types.chat import ChatCompletionToolChoiceOptionParam

from mindsdb_sdk.databases import Database
from tenacity import retry, wait_random_exponential, stop_after_attempt


DEFAULT_RETRY_MULTIPLIER = 1
DEFAULT_MAX_WAIT = 40
DEFAULT_STOP_AFTER_ATTEMPT = 3

logger = getLogger(__name__)


@retry(wait=wait_random_exponential(multiplier=DEFAULT_RETRY_MULTIPLIER, max=DEFAULT_MAX_WAIT), stop=stop_after_attempt(
    DEFAULT_RETRY_MULTIPLIER
))
def chat_completion_request(
        client: openai.OpenAI,
        model: str,
        messages: List[dict],
        tools: List = None,
        tool_choice: ChatCompletionToolChoiceOptionParam = None
):
    try:
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            tools=tools,
            tool_choice=tool_choice,
        )
        return response
    except Exception as e:
        logger.warning("Unable to generate ChatCompletion response")
        logger.warning(f"Exception: {e}")
        return e


def make_openai_tool(function: callable, description: str = None) -> dict:
    """
    Make a generic OpenAI tool for a function

    :param function: function to generate metadata for
    :param description: description of the function

    :return: dictionary containing function metadata
    """

    import inspect
    import docstring_parser

    params = inspect.signature(function).parameters
    docstring = docstring_parser.parse(function.__doc__)

    # Get the first line of the docstring as the function description or use the user-provided description
    function_description = description or docstring.short_description

    function_dict = {
        "type":"function",
        "function":{
            "name":function.__name__,
            "description":function_description,
            "parameters":{
                "type":"object",
                "properties":{},
                "required":[]
            }
        }
    }

    for name, param in params.items():
        param_description = next((p.description for p in docstring.params if p.arg_name == name), '')

        # convert annotation type to string
        if param.annotation is not inspect.Parameter.empty:
            if inspect.isclass(param.annotation):
                param_type = param.annotation.__name__
            else:
                param_type = str(param.annotation)
        else:
            param_type = None

        function_dict["function"]["parameters"]["properties"][name] = {
            "type":param_type,
            "description":param_description
        }

        # Check if parameter is required
        if param.default == inspect.Parameter.empty:
            function_dict["function"]["parameters"]["required"].append(name)

    return function_dict


def make_query_tool(schema: dict) -> dict:
    """
    Make an OpenAI tool for querying a database connection in MindsDB

    :param schema: database schema

    :return: dictionary containing function metadata for openai tools
    """
    return {
        "type":"function",
        "function":{
            "name":"query_database",
            "description":"Use this function to answer user questions. Input should be a fully formed SQL query.",
            "parameters":{
                "type":"object",
                "properties":{
                    "query":{
                        "type":"string",
                        "description":f"""
                                    SQL query extracting info to answer the user's question.
                                    SQL should be written using this database schema:
                                    {schema}
                                    The query should be returned in plain text, not in JSON.
                                    """,
                    }
                },
                "required":["query"],
            },
        }
    }


def make_data_tool(
    model: str,
    data_source: str,
    description: str,
    connection_args: dict
):
    """
    tool passing mindsdb database connection details for datasource to litellm callback

    :param model: model name for text to sql completion
    :param data_source: data source name
    :param description: description of the data source
    :param connection_args: connection arguments for the data source

    :return: dictionary containing function metadata for openai tools
    """
    # Convert the connection_args dictionary to a JSON object
    connection_args_json = json.dumps(connection_args)

    tool_description = f"""
Queries the provided data source about user data. When calling this function, ALWAYS use the following arguments:
- model: {model}
- connection_args: {connection_args_json}
- data_source: {data_source}
- description: {description}
"""

    return {
        "type":"function",
        "function":{
            "name":"get_mindsdb_text_to_sql_completion",
            "description":tool_description,
            "parameters":{
                "type":"object",
                "properties":{
                    "model":{
                        "type":"string",
                        "description":"llm model name to use for text to sql completion",
                    },
                    "data_source":{
                        "type":"string",
                        "description":"Data source name",
                    },
                    "connection_args":{
                        "type":"string",
                        "description":"Connection arguments for the data source",
                    },
                    "description":{
                        "type":"string",
                        "description":"Description of the data source",
                    }
                },
                "required": ['data_source', 'connection_args', 'model', 'description']
            }
        }
    }


def extract_sql_query(result: str) -> str:
    """
    Extract the SQL query from an openai result string

    :param result: OpenAI result string
    :return: SQL query string
    """
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


def query_database(database: Database, query: str) -> str:
    """
    Execute a query on a database connection

    :param database: mindsdb Database object
    :param query: SQL query string

    :return: query results as a string
    """
    try:
        results = str(
            database.query(query).fetch()
        )
    except Exception as e:
        results = f"query failed with error: {e}"
    return results


def execute_function_call(message, database: Database = None) -> str:
    """
    Execute a function call in a message

    """
    if message.tool_calls[0].function.name == "query_database":
        query = json.loads(message.tool_calls[0].function.arguments)["query"]
        results = query_database(database, query)
    else:
        results = f"Error: function {message.tool_calls[0].function.name} does not exist"
    return results


def pretty_print_conversation(messages):
    # you will need to pip install termcolor
    from termcolor import colored
    role_to_color = {
        "system":"red",
        "user":"green",
        "assistant":"blue",
        "function":"magenta",
    }

    for message in messages:
        if message["role"] == "system":
            logger.info(colored(f"system: {message['content']}\n", role_to_color[message["role"]]))
        elif message["role"] == "user":
            logger.info(colored(f"user: {message['content']}\n", role_to_color[message["role"]]))
        elif message["role"] == "assistant" and message.get("function_call"):
            logger.info(colored(f"assistant: {message['function_call']}\n", role_to_color[message["role"]]))
        elif message["role"] == "assistant" and not message.get("function_call"):
            logger.info(colored(f"assistant: {message['content']}\n", role_to_color[message["role"]]))
        elif message["role"] == "function":
            logger.info(colored(f"function ({message['name']}): {message['content']}\n", role_to_color[message["role"]]))