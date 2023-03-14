from typing import List

from mindsdb_sql.parser.ast import Identifier

from mindsdb_sdk.query import Query, Table


class Database:
    def __init__(self, server, name):
        self.server = server
        self.name = name
        self.api = server.api

    def __repr__(self):
        return f'{self.__class__.__name__}({self.name})'

    def query(self, sql: str) -> Query:
        return Query(self.api, sql, database=self.name)

    def _list_tables(self):
        df = self.query('show tables').fetch()

        # first column
        return list(df[df.columns[0]])

    def list_tables(self) -> List[Table]:
        return [Table(self, name) for name in self._list_tables()]

    def get_table(self, name: str) -> Table:
        if name not in self._list_tables():
            if '.' not in name:
                # fixme: schemas not visible in 'show tables'
                raise AttributeError("Table doesn't exist")
        return Table(self, name)

    def create_table(self, name: str, query: Query, replace: bool = False) -> Table:
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
