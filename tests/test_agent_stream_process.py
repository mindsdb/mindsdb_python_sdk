import pytest
import logging

from mindsdb_sdk.utils.agents import MindsDBSQLStreamParser

@pytest.fixture
def parser():
    return MindsDBSQLStreamParser(log_level=logging.INFO)

def test_initialization(parser):
    assert isinstance(parser, MindsDBSQLStreamParser)
    assert parser.logger.level == logging.INFO

def test_stream_and_parse_sql_query_with_dict(parser):
    mock_stream = [
        {'output': 'Test output', 'type': 'text'},
        {'type': 'sql', 'content': 'SELECT * FROM table'},
        {'output': 'More output'}
    ]

    generator = parser.stream_and_parse_sql_query(iter(mock_stream))
    results = list(generator)

    assert len(results) == 3
    assert results[0] == {'output': 'Test output', 'sql_query': None}
    assert results[1] == {'output': '', 'sql_query': 'SELECT * FROM table'}
    assert results[2] == {'output': 'More output', 'sql_query': None}

def test_stream_and_parse_sql_query_with_string(parser):
    mock_stream = ['String chunk 1', 'String chunk 2']

    generator = parser.stream_and_parse_sql_query(iter(mock_stream))
    results = list(generator)

    assert len(results) == 2
    assert results[0] == {'output': 'String chunk 1', 'sql_query': None}
    assert results[1] == {'output': 'String chunk 2', 'sql_query': None}


def test_process_stream(parser, caplog):
    mock_stream = [
        {'output':'First output'},
        {'type':'sql', 'content':'SELECT * FROM users'},
        {'output':'Second output'}
    ]

    with caplog.at_level(logging.INFO):
        full_response, sql_query = parser.process_stream(iter(mock_stream))

    assert full_response == 'First outputSecond output'
    assert sql_query == 'SELECT * FROM users'

    # Check for specific log messages
    assert 'Starting to process completion stream...' in caplog.text
    assert 'Output: First output' in caplog.text
    assert 'Extracted SQL Query: SELECT * FROM users' in caplog.text
    assert 'Output: Second output' in caplog.text
    assert f'Full Response: {full_response}' in caplog.text
    assert f'Final SQL Query: {sql_query}' in caplog.text

def test_process_stream_no_sql(parser):
    mock_stream = [
        {'output': 'First output'},
        {'output': 'Second output'}
    ]

    full_response, sql_query = parser.process_stream(iter(mock_stream))

    assert full_response == 'First outputSecond output'
    assert sql_query is None
