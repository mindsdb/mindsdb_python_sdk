import pandas as pd

from mindsdb_sql.parser.ast import Select, Star, Identifier, Constant

from mindsdb_sdk.utils import dict_to_binary_op


class Query:
    def __init__(self, api, sql, database=None):
        self.api = api

        self.sql = sql
        self.database = database

    def __repr__(self):
        sql = self.sql.replace('\n', ' ')
        if len(sql) > 40:
            sql = sql[:37] + '...'

        return f'{self.__class__.__name__}({sql})'

    def fetch(self) -> pd.DataFrame:
        """
        Executes query in mindsdb server and returns result
        :return: dataframe with result
        """
        return self.api.sql_query(self.sql, self.database)


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
        if len(filters) > 0:
            filters = ', '.join(
                f'{k}={v}'
                for k, v in self._filters
            )
            filters = ', ' + filters
        return filters

    def __repr__(self):
        return f'{self.__class__.__name__}({self.name}{self._filters_repr()})'

    def filter(self, **kwargs):
        """
        Applies filters on table
        table.filter(a=1, b=2) adds where condition to table:
        'select * from table1 where a=1 and b=2'

        :param kwargs: filter
        """
        self._filters.update(kwargs)
        self._update_query()

    def limit(self, val: int):
        """
        Applies limit condition to table query

        :param val: limit size
        """
        self._limit = val
        self._update_query()

    def _update_query(self):
        ast_query = Select(
            targets=[Star()],
            from_table=Identifier(self.name),
            where=dict_to_binary_op(self._filters)
        )
        if self._limit is not None:
            ast_query.limit = Constant(self._limit)
        self.sql = ast_query.to_string()


class View(Table):
    # The same as table
    pass

# TODO getting view sql from api not implemented yet
# class View(Table):
#     def __init__(self, api, data, project):
#         super().__init__(api, data['name'], project)
#         self.view_sql = data['sql']
#
#     def __repr__(self):
#         #
#         sql = self.view_sql.replace('\n', ' ')
#         if len(sql) > 40:
#             sql = sql[:37] + '...'
#
#         return f'{self.__class__.__name__}({self.name}{self._filters_repr()}, sql={sql})'
