import copy
from typing import Union
from typing import List

import pandas as pd

from mindsdb_sql.parser.ast import DropTables, CreateTable
from mindsdb_sql.parser.ast import Select, Star, Identifier, Constant, Delete, Insert, Update, Last, BinaryOperation

from mindsdb_sdk.utils.sql import dict_to_binary_op, add_condition, query_to_native_query
from mindsdb_sdk.utils.objects_collection import CollectionBase
from mindsdb_sdk.utils.context import is_saving

from .query import Query


class Table(Query):
    def __init__(self, db, name):
        # empty database
        super().__init__(db.api, '', None)
        self.name = name
        self.table_name = Identifier(parts=[db.name, name])
        self.db = db
        self._filters = {}
        self._limit = None
        self._track_column = None
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
        return f'{self.__class__.__name__}({self.table_name}{self._filters_repr()}{limit_str})'

    def filter(self, **kwargs):
        """
        Applies filters on table
        table.filter(a=1, b=2) adds where condition to table:
        'select * from table1 where a=1 and b=2'

        :param kwargs: filter
        :return: Table object
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
        :return: Table object
        """
        query = copy.deepcopy(self)
        query._limit = val
        query._update_query()
        return query

    def track(self, column):
        """
        Apply tracking column to table. ('LAST' keyword in mindsdb)
        First call returns nothing
        The next calls return new records since previous call (where value of track_column is greater)

        Example:

        >>> query = con.databases.my_db.tables.sales.filter(type='house').track('created_at')
        >>> # first call returns no records
        >>> df = query.fetch()
        >>> # second call returns rows with created_at is greater since previous fetch
        >>> df = query.fetch()

        :param column: column to track new data from table.
        :return: Table object
        """
        query = copy.deepcopy(self)
        query._track_column = column

        query._update_query()
        return query

    def _update_query(self):
        where = dict_to_binary_op(self._filters)
        if self._track_column is not None:
            condition = BinaryOperation(op='>', args=[Identifier(self._track_column), Last()])
            where = add_condition(where, condition)

        ast_query = Select(
            targets=[Star()],
            from_table=self.table_name,
            where=where
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
                table=self.table_name,
                columns=data_split['columns'],
                values=data_split['data']
            )

            sql = ast_query.to_string()

        elif isinstance(query, Query):
            # insert from select

            if query.database is not None:
                # use native query
                ast_query = Insert(
                    table=self.table_name,
                    from_select=query_to_native_query(query)
                )
                sql = ast_query.to_string()
            else:
                sql = f'INSERT INTO {self.table_name.to_string()} ({query.sql})',
        else:
            raise ValueError(f'Invalid query type: {query}')

        if is_saving():
            return Query(self, sql)

        self.api.sql_query(sql)

    def delete(self, **kwargs):
        """
        Deletes record from table using filters

        >>> table.delete(a=1, b=2)

        :param kwargs: filter
        """

        ast_query = Delete(
            table=self.table_name,
            where=dict_to_binary_op(kwargs)
        )
        sql = ast_query.to_string()

        if is_saving():
            return Query(self, sql)

        self.api.sql_query(sql)

    def update(self, values: Union[dict, Query], on: list = None, filters: dict = None):
        '''
        Update table by condition of from other table.

        If 'values' is a dict:
          it will be an update by condition
          'filters' is required
          used command: update table set a=1 where x=1

        If 'values' is a Query:
          it will be an update from select
          'on' is required
          used command: update table on a,b from (query)

        :param values: input for update, can be dict or query
        :param on: list of column to map subselect to table ['a', 'b', ...]
        :param filters: dict to filter updated rows, {'column': 'value', ...}

        '''

        if isinstance(values, Query):
            # is update from select
            if on is None:
                raise ValueError('"on" parameter is required for update from query')

            # insert from select
            if values.database is not None:
                ast_query = Update(
                    table=self.table_name,
                    keys=[Identifier(col) for col in on],
                    from_select=query_to_native_query(values)
                )
                sql = ast_query.to_string()
            else:
                map_cols = ', '.join(on)
                sql = f'UPDATE {self.table_name.to_string()} ON {map_cols} FROM ({values.sql})'

        elif isinstance(values, dict):
            # is regular update
            if filters is None:
                raise ValueError('"filters" parameter is required for update')

            update_columns = {
                k: Constant(v)
                for k, v in values.items()
            }

            ast_query = Update(
                table=self.table_name,
                update_columns=update_columns,
                where=dict_to_binary_op(filters)
            )

            sql = ast_query.to_string()
        else:
            raise NotImplementedError

        if is_saving():
            return Query(self, sql)

        self.api.sql_query(sql)


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

        return Table(self.database, name)

    def create(self, name: str, query: Union[pd.DataFrame, Query], replace: bool = False) -> Union[Table, Query]:
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

        if query.database is not None:
            # use native query
            ast_query = CreateTable(
                name=table,
                is_replace=replace,
                from_select=query_to_native_query(query)
            )
            sql = ast_query.to_string()
        else:
            replace_str = ''
            if replace:
                replace_str = ' or replace'

            sql = f'create{replace_str} table {table.to_string()} ({query.sql})'

        if is_saving():
            return Query(self, sql)

        self.api.sql_query(sql)

        return Table(self.database, name)

    def drop(self, name: str):
        """
        Delete table

        :param name: name of table
        """
        table = Identifier(parts=[self.database.name, name])

        ast_query = DropTables(
            tables=[table]
        )
        sql = ast_query.to_string()

        if is_saving():
            return Query(self, sql)
        self.api.sql_query(sql)

