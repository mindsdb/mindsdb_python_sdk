from dataclasses import dataclass
from typing import List, Union

from mindsdb_sql.parser.ast import Show, Identifier
from mindsdb_sql.parser.dialects.mindsdb import CreateMLEngine, DropMLEngine

from mindsdb_sdk.utils.objects_collection import CollectionBase

from .handlers import Handler

@dataclass
class MLEngine:
    name: str
    handler: str
    connection_data: dict


class MLEngines(CollectionBase):
    """

    **ML engines collection**

    Examples of usage:

    Get list
    >>> ml_engines = con.ml_engines.list()

    Get
    >>> openai_engine = con.ml_engines.openai1

    Create
    >>> con.ml_engines.create(
    ...    'openai1',
    ...    'openai',
    ...    connection_data={'api_key': '111'}
    ...)

    Drop
    >>>  con.ml_engines.drop('openai1')

    """

    def __init__(self, api):
        self.api = api

    def list(self) -> List[MLEngine]:
        """
        Returns list of ml engines on server
        :return: list of ml engines
        """

        ast_query = Show(category='ml_engines')

        df = self.api.sql_query(ast_query.to_string())
        # columns to lower case
        cols_map = {i: i.lower() for i in df.columns}
        df = df.rename(columns=cols_map)

        return [
            MLEngine(**item)
            for item in df.to_dict('records')
        ]

    def get(self, name: str) -> MLEngine:
        """
        Get ml engine by name

        :param name
        :return: ml engine object
        """
        name = name.lower()
        for item in self.list():
            if item.name == name:
                return item
        raise AttributeError(f"MLEngine doesn't exist {name}")

    def create(self, name: str, handler: Union[str, Handler], connection_data: dict = None) -> MLEngine:
        """
        Create new ml engine and return it
        :param name: ml engine name, string
        :param handler: handler name, string or Handler
        :param connection_data: parameters for ml engine, dict, optional
        :return: created ml engine object
        """

        if isinstance(handler, Handler):
            handler = handler.name

        ast_query = CreateMLEngine(Identifier(name), handler, params=connection_data)

        self.api.sql_query(ast_query.to_string())

        return MLEngine(name, handler, connection_data)

    def drop(self, name: str):
        """
        Drop ml engine by name
        :param name: name
        """
        ast_query = DropMLEngine(Identifier(name))

        self.api.sql_query(ast_query.to_string())

