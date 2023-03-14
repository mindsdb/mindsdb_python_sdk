
import pandas as pd

from mindsdb_sql.parser.dialects.mindsdb import *
from mindsdb_sql.parser.ast import *

from mindsdb_sdk.utils import dict_to_binary_op
from mindsdb_sdk.model import Model, ModelVersion
from mindsdb_sdk.query import Query, View


class Project:
    def __init__(self, server, name):
        self.name = name
        self.server = server
        self.api = server.api

    def __repr__(self):
        return f'{self.__class__.__name__}({self.name})'

    def query(self, sql):
        return Query(self.api, sql, database=self.name)

    def _list_views(self):
        df = self.api.objects_tree(self.name)
        df = df[df.type == 'view']

        return list(df['name'])

    def list_views(self):
        return [View(self, name) for name in self._list_views()]

    def create_view(self, name, sql, database=None):

        if isinstance(sql, Query):
            database = sql.database
            sql = sql.sql
        elif not isinstance(sql, str):
            raise ValueError()

        if database is not None:
            database = Identifier(database)
        ast_query = CreateView(
            name=Identifier(name),
            query_str=sql,
            from_table=database
        )

        self.query(ast_query.to_string()).fetch()
        return View(self, name)

    def drop_view(self, name):
        ast_query = DropView(names=[name])

        self.query(ast_query.to_string()).fetch()

    def get_view(self, name):
        if name not in self._list_views():
            raise AttributeError("View doesn't exist")
        return View(self, name)

    def list_models(self, with_versions=False, name=None, version=None):
        table = 'models'
        klass = Model
        if with_versions:
            table = 'models_versions'
            klass = ModelVersion

        filters = {}
        if name is not None:
            filters['NAME'] = name
        if version is not None:
            filters['VERSION'] = version

        ast_query = Select(
            targets=[Star()],
            from_table=Identifier(table),
            where=dict_to_binary_op(filters)
        )
        df = self.query(ast_query.to_string()).fetch()

        # columns to lower case
        cols_map = {i: i.lower() for i in df.columns}
        df = df.rename(columns=cols_map)

        return [
            klass(self, item)
            for item in df.to_dict('records')
        ]

    def create_model(self, name, predict, engine=None,
                     query=None, database=None,
                     options=None, timeseries_options=None):

        if isinstance(query, Query):
            database = query.database
            query = query.sql
        elif isinstance(query, pd.DataFrame):
            raise NotImplementedError('Dataframe as input for training model is not supported yet')

        if database is not None:
            database = Identifier(database)

        ast_query = CreatePredictor(
            name=Identifier(name),
            query_str=query,
            integration_name=database,
            targets=[Identifier(predict)],
        )

        if timeseries_options is not None:
            if 'group' in timeseries_options:
                group = timeseries_options['group']
                if not isinstance(group, list):
                    group = [group]
                ast_query.group_by = [Identifier(i) for i in group]
            if 'order' in timeseries_options:
                ast_query.order_by = [Identifier(timeseries_options['order'])]
            if 'window' in timeseries_options:
                ast_query.window = timeseries_options['window']
            if 'horizon' in timeseries_options:
                ast_query.horizon = timeseries_options['horizon']
        if options is None:
            options = {}
        if engine is not None:
            options['engine'] = engine

        df = self.query(ast_query.to_string()).fetch()
        if len(df) > 0:
            data = dict(df.iloc[0])
            # to lowercase
            data = {k.lower(): v for k,v in data.items()}

            return Model(self, data)

    def get_model(self, name, version=None):
        if version is not None:
            ret = self.list_models(with_versions=True, name=name, version=version)
        else:
            ret = self.list_models(name=name)
        if len(ret) == 0:
            raise AttributeError("Model doesn't exist")
        elif len(ret) == 1:
            return ret[0]
        else:
            raise RuntimeError('Several models with the same name/version')

    def drop_model(self, name):
        ast_query = DropPredictor(name=name)
        self.query(ast_query.to_string()).fetch()

    def drop_model_version(self, name, version):

        ast_query = Delete(
            table=Identifier('models_versions'),
            where=dict_to_binary_op({
                'name': name,
                'version': version
            })
        )
        self.query(ast_query.to_string()).fetch()

