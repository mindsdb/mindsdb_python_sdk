from __future__ import annotations

from typing import List, Union

import pandas as pd

from mindsdb_sql.parser.dialects.mindsdb import RetrainPredictor, AdjustPredictor
from mindsdb_sql.parser.ast import Identifier, Select, Star, Join, Update, Describe, Constant
from mindsdb_sql import parse_sql
from mindsdb_sql.planner.utils import query_traversal

from mindsdb_sdk.utils import dict_to_binary_op
from mindsdb_sdk.query import Query


class Model:
    def __init__(self, project, data):
        self.project = project

        self.data = data
        self.name = data['name']
        self.version = None

    def __repr__(self):
        version = ''
        if self.version is not None:
            version = f', version={self.version}'
        return f'{self.__class__.__name__}({self.name}{version}, status={self.data["status"]})'

    def _get_identifier(self):
        parts = [self.project.name, self.name]
        if self.version is not None:
            parts.append(str(self.version))
        return Identifier(parts=parts)

    def predict(self, data: Union[pd.DataFrame, Query], params: dict = None) -> pd.DataFrame:
        """
        Make prediction using model

        if data is dataframe it uses /model/predict http method and sends dataframe over it
        if data is select query with one table it replaces table to jon table and predictor
        and sends query over sql/query http method

        if data is select from join other complex query it modifies query to:
          'select from (input query) join model' and sends it over sql/query http method

        :param data: dataframe or Query object as input to predictor
        :param params: parameters for predictor, optional
        :return: dataframe with result of prediction
        """
        if isinstance(data, Query):
            # create join from select if it is simple select
            ast_query = parse_sql(data.sql, dialect='mindsdb')
            if isinstance(ast_query, Select) and isinstance(ast_query.from_table, Identifier):
                # inject aliases
                if ast_query.from_table.alias is None:
                    alias = 't'
                    ast_query.from_table.alias = Identifier(alias)
                else:
                    alias = ast_query.from_table.alias.parts[-1]

                def inject_alias(node, is_table, **kwargs):
                    if not is_table:
                        if isinstance(node, Identifier):
                            if node.parts[0] != alias:
                                node.parts.insert(0, alias)

                query_traversal(ast_query, inject_alias)

                # replace table with join
                ast_query.from_table = Join(
                    join_type='join',
                    left=ast_query.from_table,
                    right=self._get_identifier()
                )
            else:
                # wrap query to subselect
                ast_query.parentheses = True
                ast_query = Select(
                    targets=[Star()],
                    from_table=Join(
                        join_type='join',
                        left=ast_query,
                        right=self._get_identifier()
                    )
                )
            if params is not None:
                ast_query.using = params
            # execute in query's database
            return self.project.api.sql_query(ast_query.to_string(), database=data.database)

        elif isinstance(data, pd.DataFrame):
            return self.project.api.model_predict(self.project.name, self.name, data,
                                                  params=params, version=self.version)
        else:
            raise ValueError('Unknown input')

    def get_status(self) -> str:
        """
        Refresh model data and return status of model

        :return: model status
        """
        self.refresh()
        return self.data['status']

    def refresh(self):
        """
        Refresh model data from mindsdb server
        Model data can be changed during training process

        :return: model data
        """
        model = self.project.get_model(self.name, self.version)
        self.data = model.data
        return self.data

    def adjust(self,
               query: Union[str, Query] = None,
               database: str = None,
               options: dict = None,
               engine: str = None) -> Union[Model, ModelVersion]:
        """
        Call adjust of the model

        :param query: sql string or Query object to get data for adjusting, optional
        :param database: database to get data for adjusting, optional
        :param options: parameters for adjusting model, optional
        :param engine: ml engine, optional
        :return: Model object
        """
        return self._retrain(ast_class=AdjustPredictor,
                             query=query, database=database,
                             options=options, engine=engine)

    def retrain(self,
               query: Union[str, Query] = None,
               database: str = None,
               options: dict = None,
               engine: str = None) -> Union[Model, ModelVersion]:
        """
        Call retrain of the model

        :param query: sql string or Query object to get data for retraining, optional
        :param database: database to get data for retraining, optional
        :param options: parameters for retraining model, optional
        :param engine: ml engine, optional
        :return: Model object
        """
        return self._retrain(ast_class=RetrainPredictor,
                             query=query, database=database,
                             options=options, engine=engine)

    def _retrain(self,
                 ast_class,
                 query: Union[str, Query] = None,
                 database:str = None,
                 options:dict = None,
                 engine:str = None):
        if isinstance(query, Query):
            database = query.database
            query = query.sql
        elif isinstance(query, pd.DataFrame):
            raise NotImplementedError('Dataframe as input for training model is not supported yet')

        if database is not None:
            database = Identifier(database)

        if options is None:
            options = {}
        if engine is not None:
            options['engine'] = engine

        ast_query = ast_class(
            name=self._get_identifier(),
            query_str=query,
            integration_name=database,
            using=options or None,
        )

        data = self.project.query(ast_query.to_string()).fetch()
        data = {k.lower(): v for k, v in data.items()}

        # return new instance
        base_class = self.__class__
        return base_class(self.project, data)

    def describe(self, type: str = None) -> pd.DataFrame:
        """
        Return description of the model

        :param type: describe type (for lightwood is models, ensemble, features), optional
        :return: dataframe with result of description
        """
        if self.version is not None:
            raise NotImplementedError

        identifier = self._get_identifier()
        if type is not None:
            identifier.parts.append(type)
        ast_query = Describe(identifier)
        return self.project.query(ast_query.to_string()).fetch()

    def list_versions(self) -> List[ModelVersion]:
        """
        Show list of model versions

        :return: list ModelVersion objects
        """
        return self.project.list_models(with_versions=True, name=self.name)

    def get_version(self, num: int) -> ModelVersion:
        """
        Get model version by number

        :param num: version number
        :return: ModelVersion object
        """

        num = int(num)
        for m in self.project.list_models(with_versions=True, name=self.name):
            if m.version == num:
                return m
        raise ValueError('Version is not found')

    def set_active(self, version: int):
        """
        Change model active version

        :param version: version to set active
        """
        ast_query = Update(
            table=Identifier('models_versions'),
            update_columns={
                'active': Constant(1)
            },
            where=dict_to_binary_op({
                'name': self.name,
                'version': version
            })
        )
        self.project.query(ast_query.to_string()).fetch()
        self.refresh()


class ModelVersion(Model):
    def __init__(self, project, data):

        super().__init__(project, data)

        self.version = data['version']
