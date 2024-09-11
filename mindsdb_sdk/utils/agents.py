import re
import json
import logging
from typing import Dict, Any, Generator, Optional, Tuple


class MindsDBSQLStreamParser:
    """
    A utility class for parsing SQL queries from MindsDB completion streams.

    This class provides methods to process completion streams, extract SQL queries,
    and accumulate full responses.

    Attributes:
        logger (logging.Logger): The logger instance for this class.
    """

    def __init__(self, log_level: int = logging.INFO):
        """
        Initialize the MindsDBSQLStreamParser.

        Args:
            log_level (int, optional): The logging level to use. Defaults to logging.INFO.
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)

        # Create a console handler and set its level
        ch = logging.StreamHandler()
        ch.setLevel(log_level)

        # Create a formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Add the formatter to the handler
        ch.setFormatter(formatter)

        # Add the handler to the logger
        self.logger.addHandler(ch)

    def stream_and_parse_sql_query(self, completion_stream: Generator[Dict[str, Any], None, None]) -> Generator[
        Dict[str, Optional[str]], None, None]:
        """
        Stream and parse the completion stream, yielding output and SQL queries.

        This generator function processes each chunk of the completion stream,
        extracts any output and SQL queries, and yields the results.

        Args:
            completion_stream (Generator[Dict[str, Any], None, None]): The input completion stream.

        Yields:
            Dict[str, Optional[str]]: A dictionary containing 'output' and 'sql_query' keys.
                - 'output': The extracted output string from the chunk, if any.
                - 'sql_query': The extracted SQL query string, if found in the chunk.

        Note:
            This function will only yield the first SQL query it finds in the stream.
        """
        sql_query_found = False

        for chunk in completion_stream:
            output = ""
            sql_query = None

            # Log full chunk at DEBUG level
            self.logger.debug(f"Processing chunk: {json.dumps(chunk, indent=2)}")

            # Log important info at INFO level
            if isinstance(chunk, dict):
                if 'quick_response' in chunk:
                    self.logger.info(f"Quick response received: {json.dumps(chunk)}")

                output = chunk.get('output', '')
                if output:
                    self.logger.info(f"Chunk output: {output}")

                if 'messages' in chunk:
                    for message in chunk['messages']:
                        if message.get('role') == 'assistant':
                            self.logger.info(f"Assistant message: {message.get('content', '')}")
                if chunk.get('type') == 'sql':
                    sql_query = chunk['content']
                    self.logger.info(f"Generated SQL: {sql_query}")

            elif isinstance(chunk, str):
                output = chunk
                self.logger.info(f"String chunk received: {chunk}")

            yield {
                'output':output,
                'sql_query':sql_query
            }

    def process_stream(self, completion_stream: Generator[Dict[str, Any], None, None]) -> Tuple[str, Optional[str]]:
        """
        Process the completion stream and extract the SQL query.

        This method iterates through the stream, accumulates the full response,
        logs outputs, and extracts the SQL query when found.

        Args:
            completion_stream (Generator[Dict[str, Any], None, None]): The input completion stream.

        Returns:
            Tuple[str, Optional[str]]: A tuple containing:
                - The full accumulated response as a string.
                - The extracted SQL query as a string, or None if no query was found.
        """
        full_response = ""
        sql_query = None

        self.logger.info("Starting to process completion stream...")

        for result in self.stream_and_parse_sql_query(completion_stream):
            if result['output']:
                self.logger.info(f"Output: {result['output']}")
                full_response += result['output']

            if result['sql_query'] and sql_query is None:
                sql_query = result['sql_query']
                self.logger.info(f"Extracted SQL Query: {sql_query}")

        self.logger.info(f"Full Response: {full_response}")
        self.logger.info(f"Final SQL Query: {sql_query}")

        return full_response, sql_query
