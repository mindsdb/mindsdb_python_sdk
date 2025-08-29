from typing import List, Union

from mindsdb_sql.parser.dialects.mindsdb import CreateDatabase
from mindsdb_sql.parser.ast import DropDatabase, Identifier

from mindsdb_sdk.utils.objects_collection import CollectionBase
from .tree import TreeNode

from .query import Query
from .tables import Tables
from .handlers import Handler


class Database:
    """
    Allows to work with database (datasource): to use tables and make raw queries

    To run native query
    At this moment query is just saved in Qeury object and not executed

    >>> query = database.query('select * from table1') # returns Query

    This command sends request to server to execute query and return dataframe

    >>> df = query.fetch()

    Has list of tables in .tables attribute.

    """

    def __init__(self, server, name, engine=None):
        self.server = server
        self.name = name
        self.engine = engine
        self.api = server.api

        self.tables = Tables(self, self.api)

        # old api
        self.get_table = self.tables.get
        self.list_tables = self.tables.list
        self.create_table = self.tables.create
        self.drop_table = self.tables.drop

    def __repr__(self):
        return f'{self.__class__.__name__}({self.name})'

    def query(self, sql: str) -> Query:
        """
        Make raw query to integration

        :param sql: sql of the query
        :return: Query object
        """
        return Query(self.api, sql, database=self.name)
    
    def tree(self, with_schemas: bool = False) -> List[TreeNode]:
        """
        Get the tree structure of tables and schemas within this database.
        
        This returns a list of table/schema nodes with their metadata including:
        - name: table/schema name
        - class: node type ('table', 'schema', 'job')
        - type: table type ('table', 'view', 'job', 'system view')
        - engine: table engine (if applicable)
        - deletable: whether the item can be deleted
        - schema: schema name (for tables)
        - children: nested tables (for schemas)
        
        :param with_schemas: Whether to include schema information for data databases
        :return: List of TreeNode objects representing tables/schemas
        
        Example:
        
        >>> db = server.databases.get('my_postgres')
        >>> tree = db.tree(with_schemas=True)
        >>> for item in tree:
        ...     if item.class_ == 'schema':
        ...         print(f"Schema: {item.name} with {len(item.children)} tables")
        ...     else:
        ...         print(f"Table: {item.name}, Type: {item.type}")
        """
        tree_data = self.api.objects_tree(self.name, with_schemas=with_schemas)
        return [TreeNode.from_dict(item) for item in tree_data]


class Databases(CollectionBase):
    """
    Databases
    ----------

    >>> databases.list()
    >>> db = databases[0] # Database type object

    # create

    >>> db = databases.create('example_db',
    ...                                 type='postgres',
    ...                                 connection_args={'host': ''})

    # drop database

    >>> databases.drop('example_db')

    # get existing

    >>> db = databases.get('example_db')

    """

    def __init__(self, api):
        self.api = api

    def _list_databases(self):
        data = self.api.sql_query(
            "select NAME, ENGINE from information_schema.databases where TYPE='data'"
        )
        return dict(zip(data.NAME, data.ENGINE))

    def list(self) -> List[Database]:
        """
        Show list of integrations (databases) on server

        :return: list of Database objects
        """
        databases = self._list_databases()
        return [Database(self, name, engine=engine) for name, engine in databases.items()]

    def create(self, name: str, engine: Union[str, Handler], connection_args: dict) -> Database:
        """
        Create new integration and return it

        :param name: Identifier for the integration to be created
        :param engine: Engine to be selected depending on the database connection.
        :param connection_args: {"key": "value"} object with the connection parameters specific for each engine
        :return: created Database object
        """
        if isinstance(engine, Handler):
            engine = engine.name

        ast_query = CreateDatabase(
            name=Identifier(name),
            engine=engine,
            parameters=connection_args,
        )
        self.api.sql_query(ast_query.to_string())
        return Database(self, name, engine=engine)

    def drop(self, name: str):
        """
        Delete integration

        :param name: name of integration
        """
        ast_query = DropDatabase(name=Identifier(name))
        self.api.sql_query(ast_query.to_string())

    def get(self, name: str) -> Database:
        """
        Get integration by name

        :param name: name of integration
        :return: Database object
        """
        databases = self._list_databases()
        if name not in databases:
            raise AttributeError("Database doesn't exist")
        return Database(self, name, engine=databases[name])
