from typing import List

from mindsdb_sql.parser.ast import Identifier

from mindsdb_sdk.query import Query, Table


class Database:
    """
    Allows to work with database (datasource): to use tables and make raw queries

    To run native query
    At this moment query is just saved in Qeury object and not executed

    >>> query = database.query('select * from table1') # returns Query

    This command sends request to server to execute query and return dataframe

    >>> df = query.fetch()

    Wortking with tables:
    Get table as Query object

    >>> table = database.get_table('table1')

    Filter and limit

    >>> table.filter(a=1, b='2')
    >>> table.limit(3)

    Get content of table as dataframe. At that moment query will be sent on server and executed

    >>> df = table.fetch()

    Creating table

    From query:

    >>> table = database.create_table('table2', query)

    From other table

    >>> table2 = database.create_table('table2', table)

    """

    def __init__(self, server, name):
        self.server = server
        self.name = name
        self.api = server.api

    def __repr__(self):
        return f'{self.__class__.__name__}({self.name})'

    def query(self, sql: str) -> Query:
        """
        Make raw query to integration

        :param sql: sql of the query
        :return: Query object
        """
        return Query(self.api, sql, database=self.name)

    def _list_tables(self):
        df = self.query('show tables').fetch()

        # first column
        return list(df[df.columns[0]])

    def list_tables(self) -> List[Table]:
        """
        Show list of tables in integration

        :return: list of Table objects
        """
        return [Table(self, name) for name in self._list_tables()]

    def get_table(self, name: str) -> Table:
        """
        Get table by name

        :param name: name of table
        :return: Table object
        """

        if name not in self._list_tables():
            if '.' not in name:
                # fixme: schemas not visible in 'show tables'
                raise AttributeError("Table doesn't exist")
        return Table(self, name)

    def create_table(self, name: str, query: Query, replace: bool = False) -> Table:
        """
        Create new table and return it.

        On mindsdb server it executes command:
        `insert into a (select ...)`

        or if replace is True
        `create table a (select ...)`

        'select ...' is extracted from input Query

        :param name: name of table
        :param query: Query object
        :param replace: if true,
        :return: Table object
        """

        if not isinstance(query, Query):
            raise NotImplementedError

        # # query can be in different database: wrap to NativeQuery
        # ast_query = CreateTable(
        #     name=Identifier(name),
        #     is_replace=is_replace,
        #     from_select=Select(
        #         targets=[Star()],
        #         from_table=NativeQuery(
        #             integration=Identifier(data.database),
        #             query=data.sql
        #         )
        #     )
        # )
        # self.query(ast_query.to_string()).fetch()

        # call in query database
        table = Identifier(parts=[self.name, name])

        replace_str = ''
        if replace:
            replace_str = ' or replace'

        self.api.sql_query(
            f'create{replace_str} table {table.to_string()} ({query.sql})',
            database=query.database
        )

        return Table(self, name)
