from typing import List

from mindsdb_sql.parser.dialects.mindsdb import CreateDatabase
from mindsdb_sql.parser.ast import DropDatabase

from mindsdb_sdk.connectors.rest_api import RestAPI
from .database import Database
from .project import Project


class Server:
    """
    Server instance allows to manipulate project and databases (integration) on mindsdb server
    Example if usage:

    Databases
    ----------

    >>> databases = server.list_databases()
    >>> database = databases[0] # Database type object

    # create

    >>> database = server.create_database('example_db',
    ...                                 type='postgres',
    ...                                 connection_args={'host': ''})

    # drop database

    >>> server.drop_database('example_db')

    # get existing

    >>> database = server.get_database('example_db')

    Projects
    ----------

    # list of projects

    >>> projects = server.list_projects()
    >>> project = projects[0]  # Project type object


    # create

    >>> project = server.create_project('proj')

    # drop

    >>> server.drop_project('proj')

    # get existing

    >>> project = server.get_project('proj')

    >>> project = server.get_project()  # default is mindsdb project

    """

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
        """
        Show list of integrations (databases) on server

        :return: list of Database objects
        """
        return [Database(self, name) for name in self._list_databases()]

    def create_database(self, name: str, engine: str, connection_args: dict) -> Database:
        """
        Create new integration and return it

        :param name: Identifier for the integration to be created
        :param engine: Engine to be selected depending on the database connection.
        :param connection_args: {"key": "value"} object with the connection parameters specific for each engine
        :return: created Database object
        """
        ast_query = CreateDatabase(
            name=name,
            engine=engine,
            parameters=connection_args,
        )
        self.api.sql_query(ast_query.to_string())
        return Database(self, name)

    def drop_database(self, name: str):
        """
        Delete integration

        :param name: name of integration
        """
        ast_query = DropDatabase(name=name)
        self.api.sql_query(ast_query.to_string())

    def get_database(self, name: str) -> Database:
        """
        Get integration by name

        :param name: name of integration
        :return: Database object
        """
        if name not in self._list_databases():
            raise AttributeError("Database doesn't exist")
        return Database(self, name)

    def _list_projects(self):
        data = self.api.sql_query("select NAME from information_schema.databases where TYPE='project'")
        return list(data.NAME)

    def list_projects(self) -> List[Project]:
        """
        Show list of project on server

        :return: list of Project objects
        """
        # select * from information_schema.databases where TYPE='project'
        return [Project(self, name) for name in self._list_projects()]

    def create_project(self, name: str) -> Project:
        """
        Create new project and return it

        :param name: name of the project
        :return: Project object
        """

        ast_query = CreateDatabase(
            name=name,
            engine='mindsdb',
            parameters={}
        )

        self.api.sql_query(ast_query.to_string())
        return Project(self, name)

    def drop_project(self, name: str):
        """
        Drop project from server

        :param name: name of the project
        """
        ast_query = DropDatabase(name=name)
        self.api.sql_query(ast_query.to_string())

    def get_project(self, name: str = 'mindsdb') -> Project:
        """
        Get Project by name

        :param name: name of project
        :return: Project object
        """
        if name not in self._list_projects():
            raise AttributeError("Database doesn't exist")
        return Project(self, name)
