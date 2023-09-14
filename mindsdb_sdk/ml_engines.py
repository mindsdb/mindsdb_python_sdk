from dataclasses import dataclass
from typing import List

from mindsdb_sql.parser.ast import Show, Identifier
from mindsdb_sql.parser.dialects.mindsdb import CreateMLEngine, DropMLEngine

from mindsdb_sdk.utils.objects_collection import CollectionBase


@dataclass
class MLEngine:
    name: str
    handler: str
    connection_data: dict


class MLEngines(CollectionBase):

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
        raise AttributeError("MLEngine doesn't exist")

    def create(self, name: str, handler: str, connection_data: dict =None) -> MLEngine:
        """
        Create new ml engine and return it
        :param name: ml engine name
        :param handler: handler name
        :param connection_data: parameters for ml engine, optional
        :return: created ml engine object
        """
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

