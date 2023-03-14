from typing import List

from mindsdb_sql.parser.dialects.mindsdb import CreateDatabase
from mindsdb_sql.parser.ast import DropDatabase

from mindsdb_sdk.connectors.rest_api import RestAPI
from .database import Database
from .project import Project


class Server:

    def __init__(self, url: str = None, email: str = None, password: str = None):
        self.api = RestAPI(url, email, password)

    def __repr__(self):
        return f'{self.__class__.__name__}({self.api.url})'

    def _list_databases(self):
        data = self.api.sql_query(
            "select NAME from information_schema.databases where TYPE='data'"
        )
        return list(data.NAME)

    def list_databases(self) -> List[Database]:
        return [Database(self, name) for name in self._list_databases()]

    def create_database(self, name: str, engine: str, connection_args: dict) -> Database:
        ast_query = CreateDatabase(
            name=name,
            engine=engine,
            parameters=connection_args,
        )
        self.api.sql_query(ast_query.to_string())
        return Database(self, name)

    def drop_database(self, name: str):
        ast_query = DropDatabase(name=name)
        self.api.sql_query(ast_query.to_string())

    def get_database(self, name: str) -> Database:
        if name not in self._list_databases():
            raise AttributeError("Database doesn't exist")
        return Database(self, name)

    def _list_projects(self):
        data = self.api.sql_query("select NAME from information_schema.databases where TYPE='project'")
        return list(data.NAME)

    def list_projects(self) -> List[Project]:
        # select * from information_schema.databases where TYPE='project'
        return [Project(self, name) for name in self._list_projects()]

    def create_project(self, name: str) -> Project:
        ast_query = CreateDatabase(
            name=name,
            engine='mindsdb',
            parameters={}
        )

        self.api.sql_query(ast_query.to_string())
        return Project(self, name)

    def drop_project(self, name: str):
        ast_query = DropDatabase(name=name)
        self.api.sql_query(ast_query.to_string())

    def get_project(self, name: str = 'mindsdb') -> Project:
        if name not in self._list_projects():
            raise AttributeError("Database doesn't exist")
        return Project(self, name)
