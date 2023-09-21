import pandas as pd


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

