import json
from unittest.mock import patch, MagicMock
from mindsdb_sdk.utils import openai


def test_chat_completion_request_success():
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = "Test Response"
    response = openai.chat_completion_request(mock_client, "text-davinci-002", [{"role": "system", "content": "You are a helpful assistant."}])
    assert response == "Test Response"


def test_make_openai_tool():
    def test_func(a: int, b: str) -> str:
        """This is a test function"""
        return b * a
    tool = openai.make_openai_tool(test_func)
    assert tool["function"]["name"] == "test_func"
    assert tool["function"]["description"] == "This is a test function"
    assert tool["function"]["parameters"]["properties"]["a"]["type"] == "int"
    assert tool["function"]["parameters"]["properties"]["b"]["type"] == "str"


def test_extract_sql_query():
    result = "SQLQuery: SELECT * FROM test_table\nSQLResult: [{'column1': 'value1', 'column2': 'value2'}]"
    query = openai.extract_sql_query(result)
    assert query == "SELECT * FROM test_table"


def test_extract_sql_query_no_query():
    result = "SQLResult: [{'column1': 'value1', 'column2': 'value2'}]"
    query = openai.extract_sql_query(result)
    assert query is None


@patch("mindsdb_sdk.utils.openai.query_database")
def test_execute_function_call_query_database(mock_query_database):
    mock_query_database.return_value = "Test Result"
    mock_message = MagicMock()
    mock_message.tool_calls[0].function.name = "query_database"
    mock_message.tool_calls[0].function.arguments = json.dumps({"query": "SELECT * FROM test_table"})
    result = openai.execute_function_call(mock_message, MagicMock())
    assert result == "Test Result"


def test_execute_function_call_no_function():
    mock_message = MagicMock()
    mock_message.tool_calls[0].function.name = "non_existent_function"
    result = openai.execute_function_call(mock_message, MagicMock())
    assert result == "Error: function non_existent_function does not exist"
