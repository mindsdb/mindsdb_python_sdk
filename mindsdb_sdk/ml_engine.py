from dataclasses import dataclass

from mindsdb_sql.parser.ast import Show, Identifier
from mindsdb_sql.parser.dialects.mindsdb import CreateMLEngine, DropMLEngine


from .objects_collection import CollectionBase


@dataclass
class MLEngine:
    name: str
    handler: str
    connection_data: dict


class MLEngines(CollectionBase):

    def __init__(self, api):
        self.api = api

    def list(self):

        ast_query = Show(category='ml_engines')

        df = self.api.sql_query(ast_query.to_string())
        # columns to lower case
        cols_map = {i: i.lower() for i in df.columns}
        df = df.rename(columns=cols_map)

        return [
            MLEngine(**item)
            for item in df.to_dict('records')
        ]

    def get(self, name):
        name = name.lower()
        for item in self.list():
            if item.name == name:
                return item
        raise AttributeError("MLEngine doesn't exist")

    def create(self, name, handler, connection_data=None):
        ast_query = CreateMLEngine(Identifier(name), handler, params=connection_data)

        self.api.sql_query(ast_query.to_string())

        return MLEngine(name, handler, connection_data)

    def drop(self, name):
        ast_query = DropMLEngine(Identifier(name))

        self.api.sql_query(ast_query.to_string())

