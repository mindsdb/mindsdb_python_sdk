import copy
from typing import Union
from typing import List

import pandas as pd

from mindsdb_sql.parser.ast import DropTables
from mindsdb_sql.parser.ast import Select, Star, Identifier, Constant, Delete, Insert, Update

from mindsdb_sdk.utils.sql import dict_to_binary_op
from mindsdb_sdk.utils.objects_collection import CollectionBase

from .query import Query


class Table(Query):
    def __init__(self, db, name):
        super().__init__(db.api, '', db.name)
        self.name = name
        self.db = db
        self._filters = {}
        self._limit = None
        self._update_query()

    def _filters_repr(self):
        filters = ''
        if len(self._filters) > 0:
            filters = ', '.join(
                f'{k}={v}'
                for k, v in self._filters.items()
            )
            filters = ', ' + filters
        return filters

    def __repr__(self):
        limit_str = ''
        if self._limit is not None:
            limit_str = f'; limit={self._limit}'
        return f'{self.__class__.__name__}({self.name}{self._filters_repr()}{limit_str})'

    def filter(self, **kwargs):
        """
        Applies filters on table
        table.filter(a=1, b=2) adds where condition to table:
        'select * from table1 where a=1 and b=2'

        :param kwargs: filter
        """
        # creates new object
        query = copy.deepcopy(self)
        query._filters.update(kwargs)
        query._update_query()
        return query

    def limit(self, val: int):
        """
        Applies limit condition to table query

        :param val: limit size
        """
        query = copy.deepcopy(self)
        query._limit = val
        query._update_query()
        return query

    def _update_query(self):
        ast_query = Select(
            targets=[Star()],
            from_table=Identifier(self.name),
            where=dict_to_binary_op(self._filters)
        )
        if self._limit is not None:
            ast_query.limit = Constant(self._limit)
        self.sql = ast_query.to_string()

    def insert(self, query: Union[pd.DataFrame, Query]):
        """
        Insert data from query of dataframe
        :param query: dataframe of
        :return:
        """

        if isinstance(query, pd.DataFrame):
            # insert data
            data_split = query.to_dict('split')

            ast_query = Insert(
                table=Identifier(self.name),
                columns=data_split['columns'],
                values=data_split['data']
            )

            sql = ast_query.to_string()
            self.api.sql_query(sql, self.database)
        else:
            # insert from select
            table = Identifier(parts=[self.database, self.name])
            self.api.sql_query(
                f'INSERT INTO {table.to_string()} ({query.sql})',
                database=query.database
            )

    def delete(self, **kwargs):
        """
        Deletes record from table using filters  table.delete(a=1, b=2)

        :param kwargs: filter
        """
        identifier = Identifier(self.name)
        # add database
        identifier.parts.insert(0, self.database)

        ast_query = Delete(
            table=identifier,
            where=dict_to_binary_op(kwargs)
        )

        sql = ast_query.to_string()
        self.api.sql_query(sql, 'mindsdb')

    def update(self, values: Union[dict, Query], on: list = None, filters: dict = None):
        '''
        Update table by condition of from other table.
        If 'values' is a dict:
        - it will be an update by condition
        - 'filters' is required
        - used command: update table set a=1 where x=1

        If 'values' is a Query:
        - it will be an update from select
        - 'on' is required
        - used command: update table on a,b from (query)

        :param values: input for update, can be dict or query
        :param on: list of column to map subselect to table ['a', 'b', ...]
        :param filters: dict to filter updated rows, {'column': 'value', ...}

        '''

        if isinstance(values, Query):
            # is update from select
            if on is None:
                raise ValueError('"on" parameter is required for update from query')

            # insert from select
            table = Identifier(parts=[self.database, self.name])
            map_cols = ', '.join(on)
            self.api.sql_query(
                f'UPDATE {table.to_string()} ON {map_cols} FROM ({values.sql})',
                database=values.database
            )
        elif isinstance(values, dict):
            # is regular update
            if filters is None:
                raise ValueError('"filters" parameter is required for update')

            update_columns = {
                k: Constant(v)
                for k, v in values.items()
            }

            ast_query = Update(
                table=Identifier(self.name),
                update_columns=update_columns,
                where=dict_to_binary_op(filters)
            )

            sql = ast_query.to_string()
            self.api.sql_query(sql, self.database)
        else:
            raise NotImplementedError


class Tables(CollectionBase):
    """
    Wortking with tables:
    Get table as Query object

    >>> table = tables.get('table1')

    Filter and limit

    >>> table = table.filter(a=1, b='2')
    >>> table = table.limit(3)

    Get content of table as dataframe. At that moment query will be sent on server and executed

    >>> df = table.fetch()

    Creating table

    From query:

    >>> table = tables.create('table2', query)

    From other table

    >>> table2 = table.create('table2', table)

    Uploading file

    >>> db = con.databases.files
    >>> db.tables.create('filename', dataframe)

  ` Droping table

    >>> db.tables.drop('table2')
    """

    def __init__(self, database, api):
        self.database = database
        self.api = api

    def _list_tables(self):
        df = self.database.query('show tables').fetch()

        # first column
        return list(df[df.columns[0]])

    def list(self) -> List[Table]:
        """
        Show list of tables in integration

        :return: list of Table objects
        """
        return [Table(self.database, name) for name in self._list_tables()]

    def get(self, name: str) -> Table:
        """
        Get table by name

        :param name: name of table
        :return: Table object
        """

        if name not in self._list_tables():
            if '.' not in name:
                # fixme: schemas not visible in 'show tables'
                raise AttributeError("Table doesn't exist")
        return Table(self.database, name)

    def create(self, name: str, query: Union[pd.DataFrame, Query], replace: bool = False) -> Table:
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

        if isinstance(query, pd.DataFrame) and self.database.name == 'files':
            # now it is only possible for file uploading
            self.api.upload_file(name, query)

            return Table(self.database, name)

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
        table = Identifier(parts=[self.database.name, name])

        replace_str = ''
        if replace:
            replace_str = ' or replace'

        self.api.sql_query(
            f'create{replace_str} table {table.to_string()} ({query.sql})',
            database=query.database
        )

        return Table(self.database, name)

    def drop(self, name: str):
        """
        Delete table

        :param name: name of table
        """
        ast_query = DropTables(
            tables=[
                Identifier(parts=[name])
            ]
        )
        self.api.sql_query(ast_query.to_string(), database=self.database.name)

