from __future__ import annotations

import time
from typing import List, Union

import pandas as pd

from mindsdb_sql.parser.dialects.mindsdb import CreatePredictor, DropPredictor
from mindsdb_sql.parser.dialects.mindsdb import RetrainPredictor, FinetunePredictor
from mindsdb_sql.parser.ast import Identifier, Select, Star, Join, Describe, Set
from mindsdb_sql import parse_sql
from mindsdb_sql.exceptions import ParsingException

from .ml_engines import MLEngine

from mindsdb_sdk.utils.objects_collection import CollectionBase
from mindsdb_sdk.utils.sql import dict_to_binary_op, query_to_native_query
from mindsdb_sdk.utils.context import is_saving

from .query import Query


class Model:
    """

    Versions

    List model versions

    >>> model.list_versions()


    Get info

    >>> print(model.get_status())
    >>> print(model.data)

    Update model data from server

    >>> model.refresh()

    **Usng model**

    Dataframe on input

    >>> result_df = model.predict(df_rental)
    >>> result_df = model.predict(df_rental, params={'a': 'q'})

    Dict on input

    >>> result_df = model.predict({'n_rooms': 2})

    Deferred query on input

    >>> result_df = model.predict(query, params={'': ''})

    Time series prediction

    >>> query = database.query('select * from table1 where type="house" and saledate>latest')
    >>> model.predict(query)

    The join model with table in raw query

    >>> result_df = project.query('''
    ...  SELECT m.saledate as date, m.ma as forecast
    ...   FROM mindsdb.house_sales_model as m
    ...   JOIN example_db.demo_data.house_sales as t
    ...  WHERE t.saledate > LATEST AND t.type = 'house'
    ...   AND t.bedrooms=2
    ...  LIMIT 4;
    ...''').fetch()


    **Model managing**

    Fine-tuning

    >>> model.finetune(query)
    >>> model.finetune('select * from demo_data.house_sales', database='example_db')
    >>> model.finetune(query, params={'x': 2})

    Retraining

    >>> model.retrain(query)
    >>> model.retrain('select * from demo_data.house_sales', database='example_db')
    >>> model.retrain(query, params={'x': 2})

    Describe

    >>> df_info = model.describe()
    >>> df_info = model.describe('features')

    Change active version

    >>> model.set_active(version=3)

    """

    def __init__(self, project, data):
        self.project = project

        self.data = data
        self.name = data['name']
        self.version = None

    def __repr__(self):
        version = ''
        if self.version is not None:
            version = f', version={self.version}'
        return f'{self.__class__.__name__}({self.name}{version}, status={self.data.get("status")})'

    def _get_identifier(self):
        parts = [self.project.name, self.name]
        if self.version is not None:
            parts.append(str(self.version))
        return Identifier(parts=parts)

    def predict(self, data: Union[pd.DataFrame, Query, dict], params: dict = None) -> Union[pd.DataFrame, Query]:
        """
        Make prediction using model

        if data is dataframe
          it uses /model/predict http method and sends dataframe over it

        if data is select query with one table
         it replaces table to jon table and predictor and sends query over sql/query http method

        if data is select from join other complex query it modifies query to:
          'select from (input query) join model' and sends it over sql/query http method

        :param data: dataframe or Query object as input to predictor
        :param params: parameters for predictor, optional
        :return: dataframe with result of prediction
        """

        if isinstance(data, Query):
            # create join from select if it is simple select
            try:
                ast_query = parse_sql(data.sql, dialect='mindsdb')
            except ParsingException:
                ast_query = None

            # injection of join disabled yet
            # if isinstance(ast_query, Select) and isinstance(ast_query.from_table, Identifier):
            #     # inject aliases
            #     if ast_query.from_table.alias is None:
            #         alias = 't'
            #         ast_query.from_table.alias = Identifier(alias)
            #     else:
            #         alias = ast_query.from_table.alias.parts[-1]
            #
            #     def inject_alias(node, is_table, **kwargs):
            #         if not is_table:
            #             if isinstance(node, Identifier):
            #                 if node.parts[0] != alias:
            #                     node.parts.insert(0, alias)
            #
            #     query_traversal(ast_query, inject_alias)
            #
            #     # replace table with join
            #     model_identifier = self._get_identifier()
            #     model_identifier.alias = Identifier('m')
            #
            #     ast_query.from_table = Join(
            #         join_type='join',
            #         left=ast_query.from_table,
            #         right=model_identifier
            #     )
            #
            #     # select only model columns
            #     ast_query.targets = [Identifier(parts=['m', Star()])]
            #

            model_identifier = self._get_identifier()
            model_identifier.alias = Identifier('m')

            if data.database is not None or ast_query is None or not isinstance(ast_query, Select):
                # use native query
                native_query = query_to_native_query(data)
                native_query.parentheses = True
                native_query.alias = Identifier('t')
                upper_query = Select(
                    targets=[Identifier(parts=['m', Star()])],
                    from_table=Join(
                        join_type='join',
                        left=native_query,
                        right=model_identifier
                    )
                )
            else:
                # wrap query to subselect
                model_identifier = self._get_identifier()
                model_identifier.alias = Identifier('m')

                ast_query.parentheses = True
                ast_query.alias = Identifier('t')
                upper_query = Select(
                    targets=[Identifier(parts=['m', Star()])],
                    from_table=Join(
                        join_type='join',
                        left=ast_query,
                        right=model_identifier
                    )
                )
            if params is not None:
                upper_query.using = params
            # execute in query's database
            sql = upper_query.to_string()
            if is_saving():
                return Query(self, sql)

            return self.project.api.sql_query(sql, database=None)

        elif isinstance(data, dict):
            data = pd.DataFrame([data])
            return self.project.api.model_predict(self.project.name, self.name, data,
                                                  params=params, version=self.version)
        elif isinstance(data, pd.DataFrame):
            return self.project.api.model_predict(self.project.name, self.name, data,
                                                  params=params, version=self.version)
        else:
            raise ValueError('Unknown input')

    def wait_complete(self):

        for i in range(400):
            time.sleep(0.3)

            status = self.get_status()
            if status in ('generating', 'training'):
                continue
            elif status == 'error':
                raise RuntimeError(f'Training failed: {self.data["error"]}')
            else:
                break

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

    def finetune(self,
               query: Union[str, Query] = None,
               database: str = None,
               options: dict = None,
               engine: str = None) -> Union[Model, ModelVersion]:
        """
        Call finetune of the model

        :param query: sql string or Query object to get data for fine-tuning, optional
        :param database: database to get data for fine-tuning, optional
        :param options: parameters for fine-tuning model, optional
        :param engine: ml engine, optional
        :return: Model object
        """
        return self._retrain(ast_class=FinetunePredictor,
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
        sql = ast_query.to_string()

        if is_saving():
            return Query(self, sql)

        data = self.project.api.sql_query(sql)
        data = {k.lower(): v for k, v in data.items()}

        # return new instance
        base_class = self.__class__
        return base_class(self.project, data)

    def describe(self, type: str = None) -> Union[pd.DataFrame, Query]:
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

        sql = ast_query.to_string()
        if is_saving():
            return Query(self, sql)

        return self.project.api.sql_query(sql)

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

    def drop_version(self, num: int) -> ModelVersion:
        """
        Drop version of the model

        >>> models.rentals_model.drop_version(version=10)

        :param num: version to drop
        """

        return self.project.drop_model_version(self.name, num)

    def set_active(self, version: int):
        """
        Change model active version

        :param version: version to set active
        """
        ast_query = Set(
            category='active',
            value=Identifier(parts=[self.project.name, self.name, str(version)])
        )
        sql = ast_query.to_string()
        if is_saving():
            return Query(self, sql)

        self.project.api.sql_query(sql)
        self.refresh()


class ModelVersion(Model):
    def __init__(self, project, data):

        super().__init__(project, data)

        self.version = data['version']


class Models(CollectionBase):
    """

    **Models**

    Get:

    >>> all_models = models.list()
    >>> model = all_models[0]

    Get version:

    >>> all_models = models.list(with_versions=True)
    >>> model = all_models[0]

    By name:

    >>> model = models.get('model1')
    >>> model = models.get('model1', version=2)

    """

    def __init__(self, project, api):
        self.project = project
        self.api = api

    def create(
        self,
        name: str,
        predict: str = None,
        engine: Union[str, MLEngine] = None,
        query: Union[str, Query] = None,
        database: str = None,
        options: dict = None,
        timeseries_options: dict = None, **kwargs
    ) -> Union[Model, Query]:
        """
        Create new model in project and return it

        If query/database is passed, it will be executed on mindsdb side

        Create, using params and qeury as string

        >>> model = models.create(
        ...   'rentals_model',
        ...   predict='price',
        ...   engine='lightwood',
        ...   database='example_db',
        ...   query='select * from table',
        ...   options={
        ...       'module': 'LightGBM'
        ...   },
        ...   timeseries_options={
        ...       'order': 'date',
        ...       'group': ['a', 'b']
        ...   }
        ...)
    
        Create, using deferred query. 'query' will be executed and converted to dataframe on mindsdb backend.

        >>> query = databases.db.query('select * from table')
        >>> model = models.create(
        ...   'rentals_model',
        ...   predict='price',
        ...   query=query,
        ...)

        :param name: name of the model
        :param predict: prediction target
        :param engine: ml engine for new model, default is mindsdb
        :param query: sql string or Query object to get data for training of model, optional
        :param database: database to get data for training, optional
        :param options: parameters for model, optional
        :param timeseries_options: parameters for forecasting model
        :return: created Model object, it can be still in training state
        """
        if isinstance(query, Query):
            database = query.database
            query = query.sql
        elif isinstance(query, pd.DataFrame):
            raise NotImplementedError('Dataframe as input for training model is not supported yet')

        if database is not None:
            database = Identifier(database)

        if predict is not None:
            targets = [Identifier(predict)]
        else:
            targets = None

        ast_query = CreatePredictor(
            name=Identifier(parts=[self.project.name, name]),
            query_str=query,
            integration_name=database,
            targets=targets,
        )

        if timeseries_options is not None:
            # check ts options
            allowed_keys = ['group', 'order', 'window', 'horizon']
            for key in timeseries_options.keys():
                if key not in allowed_keys:
                    raise AttributeError(f"Unexpected time series option: {key}")

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
        # options and kwargs are the same
        options.update(kwargs)

        if engine is not None:
            if isinstance(engine, MLEngine):
                engine = engine.name

            options['engine'] = engine
        ast_query.using = options

        sql = ast_query.to_string()

        if is_saving():
            return Query(self, sql)

        df = self.project.api.sql_query(sql)
        if len(df) > 0:
            data = dict(df.iloc[0])
            # to lowercase
            data = {k.lower(): v for k,v in data.items()}

            return Model(self.project, data)

    def get(self, name: str, version: int = None) -> Union[Model, ModelVersion]:
        """
         Get model by name from project

         if version is passed it returns ModelVersion object with specific version

        :param name: name of the model
        :param version: version of model, optional
        :return: Model or ModelVersion object
        """
        if version is not None:
            ret = self.list(with_versions=True, name=name, version=version)
        else:
            ret = self.list(name=name)
        if len(ret) == 0:
            raise AttributeError("Model doesn't exist")
        elif len(ret) == 1:
            return ret[0]
        else:
            raise RuntimeError('Several models with the same name/version')

    def drop(self, name: str):
        """
        Drop model from project with all versions

        >>> models.drop('rentals_model')

        :param name: name of the model
        """
        ast_query = DropPredictor(name=Identifier(parts=[self.project.name, name]))
        sql = ast_query.to_string()
        if is_saving():
            return Query(self, sql)

        self.project.api.sql_query(sql)


    def list(self, with_versions: bool = False,
                    name: str = None,
                    version: int = None) -> List[Union[Model, ModelVersion]]:
        """
        List models (or model versions) in project

        If with_versions = True
          it shows all models with version (executes 'select * from models_versions')

          Otherwise it shows only models (executes 'select * from models')

        :param with_versions: show model versions
        :param name: to show models or versions only with selected name, optional
        :param version: to show model or versions only with selected version, optional
        :return: list of Model or ModelVersion objects
        """

        model_class = Model

        filters = {}
        if name is not None:
            filters['NAME'] = name
        if version is not None:
            filters['VERSION'] = version

        if with_versions:
            model_class = ModelVersion
        else:
            filters['ACTIVE'] = '1'

        ast_query = Select(
            targets=[Star()],
            from_table=Identifier('models'),
            where=dict_to_binary_op(filters)
        )
        df = self.project.query(ast_query.to_string()).fetch()

        # columns to lower case
        cols_map = { i: i.lower() for i in df.columns }
        df = df.rename(columns=cols_map)

        return [
            model_class(self.project, item)
            for item in df.to_dict('records')
        ]