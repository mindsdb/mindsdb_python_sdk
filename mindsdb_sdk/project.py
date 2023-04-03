import datetime as dt
from typing import Union, List

import pandas as pd

from mindsdb_sql.parser.dialects.mindsdb import CreatePredictor, CreateView, DropPredictor, CreateJob, DropJob
from mindsdb_sql.parser.ast import DropView, Identifier, Delete, Star, Select

from mindsdb_sdk.utils import dict_to_binary_op
from mindsdb_sdk.model import Model, ModelVersion
from mindsdb_sdk.query import Query, View


class Job:
    def __init__(self, project, data):
        self.project = project
        self.data = data
        self._update(data)

    def _update(self, data):
        self.name = data['name']
        self.query_str = data['query']
        self.start_at = data['start_at']
        self.end_at = data['end_at']
        self.next_run_at = data['next_run_at']
        self.schedule_str = data['schedule_str']

    def __repr__(self):
        return f"{self.__class__.__name__}({self.name}, query='{self.query_str}')"

    def refresh(self):
        job = self.project.get_job(self.name)
        self._update(job.data)

    def get_history(self):
        ast_query = Select(
            targets=[Star()],
            from_table=Identifier('jobs_history'),
            where=dict_to_binary_op({
                'name': self.name
            })
        )
        return self.project.api.sql_query(ast_query.to_string(), database=self.project.name)


class Project:
    """
    Allows to work with project: to manage models and views inside of it or call raw queries inside of project

    Queries
    ----------
    Making prediciton using sql:

    >>> query = project.query('select * from database.table join model1')
    >>> df = query.fetch()

    Making time series prediction:

    >>> df = project.query('''
    ...      SELECT m.saledate as date, m.ma as forecast
    ...     FROM mindsdb.house_sales_model as m
    ...     JOIN example_db.demo_data.house_sales as t
    ...     WHERE t.saledate > LATEST AND t.type = 'house'
    ...     AND t.bedrooms=2
    ...     LIMIT 4;
    ...    ''').fetch()

    Views
    ----------
    Get:

    >>> views = project.list_views()
    >>> view = views[0]

    By name:

    >>> view project.get_view('view1')

    Create:

    >>> view = project.create_view(
    ...   'view1',
    ...   database='example_db',  # optional, can also be database object
    ...   query='select * from table1'
    ...)

    Create using query object:

    >>> view = project.create_view(
    ...   'view1',
    ...   query=database.query('select * from table1')
    ...)

    Getting data:

    >>> view.filter(a=1, b=2)
    >>> view.limit(100)
    >>> df = view.fetch()

    Drop view:

    >>> project.drop_view('view1')


    Models
    ----------

    Get:

    >>> models = project.list_models()
    >>> model = models[0]

    Get version:

    >>> models = project.list_models(with_versions=True)
    >>> model = models[0]

    By name:

    >>> model = project.get_model('model1')
    >>> model = project.get_model('model1', version=2)

    Versions

    List model versions

    >>> models = model.list_versions()
    >>> model = models[0]  # Model object


    Get info

    >>> print(model.status)
    >>> print(model.data)

    Update model data from server

    >>> model.refresh()

    Create

    Create, using params and qeury as string

    >>> model = project.create_model(
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

    >>> query = database.query('select * from table')
    >>> model = project.create_model(
    ...   'rentals_model',
    ...   predict='price',
    ...   query=query,
    ...)

    Usng model

    Dataframe on input

    >>> result_df = model.predict(df_rental)
    >>> result_df = model.predict(df_rental, params={'a': 'q'})

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


    Model managing

    Adjusting

    >>> model.adjust(query)
    >>> model.adjust('select * from demo_data.house_sales', database='example_db')
    >>> model.adjust(query, params={'x': 2})

    Retraining

    >>> model.retrain(query)
    >>> model.retrain('select * from demo_data.house_sales', database='example_db')
    >>> model.retrain(query, params={'x': 2})

    Describe

    >>> df_info = model.describe()
    >>> df_info = model.describe('features')

    Change active version

    >>> model.set_active(version=3)

    Drop

    >>> project.drop_model('rentals_model')
    >>> project.drop_model_version('rentals_model', version=10)

    """

    def __init__(self, server, name):
        self.name = name
        self.server = server
        self.api = server.api

    def __repr__(self):
        return f'{self.__class__.__name__}({self.name})'

    def query(self, sql: str) -> Query:
        """
        Execute raw query inside of project

        :param sql: sql query
        :return: Query object
        """
        return Query(self.api, sql, database=self.name)

    def _list_views(self):
        df = self.api.objects_tree(self.name)
        df = df[df.type == 'view']

        return list(df['name'])

    def list_views(self) -> List[View]:
        """
        Show list of views in project

        :return: list of View objects
        """
        return [View(self, name) for name in self._list_views()]

    def create_view(self, name: str, sql: Union[str, Query], database: str = None) -> View:
        """
        Create new view in project and return it

        :param name: name of the view
        :param sql: sql query as string or query object
        :param database: datasource of the view (where input sql will be executed)
        :return: View object
        """
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

    def drop_view(self, name: str):
        """
        Drop view from project

        :param name: name of the view
        """

        ast_query = DropView(names=[name])

        self.query(ast_query.to_string()).fetch()

    def get_view(self, name: str) -> View:
        """
        Get view by name from project

        :param name: name of the view
        :return: View object
        """

        if name not in self._list_views():
            raise AttributeError("View doesn't exist")
        return View(self, name)

    def list_models(self, with_versions: bool = False,
                    name: str = None,
                    version: int = None) -> List[Union[Model, ModelVersion]]:
        """
        List models (or model versions) in project

        If with_versions = True it shows all models with version (executes 'select * from models_versions')
        Otherwise it shows only models (executes 'select * from models')

        :param with_versions: show model versions
        :param name: to show models or versions only with selected name, optional
        :param version: to show model or versions only with selected version, optional
        :return: list of Model or ModelVersion objects
        """

        table = 'models'
        model_class = Model
        if with_versions:
            table = 'models_versions'
            model_class = ModelVersion

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
            model_class(self, item)
            for item in df.to_dict('records')
        ]

    def create_model(self, name: str, predict: str, engine: str = None,
                     query: Union[str, Query] = None, database: str = None,
                     options: dict = None, timeseries_options: dict = None) -> Model:
        """
        Create new model in project and return it

        If query/database is passed, it will be executed on mindsdb side

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

    def get_model(self, name: str, version: int = None) -> Union[Model, ModelVersion]:
        """
         Get model by name from project

         if version is passed it returns ModelVersion object with specific version

        :param name: name of the model
        :param version: version of model, optional
        :return: Model or ModelVersion object
        """
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

    def drop_model(self, name: str):
        """
        Drop model from project with all versions

        :param name: name of the model
        """
        ast_query = DropPredictor(name=name)
        self.query(ast_query.to_string()).fetch()

    def drop_model_version(self, name: str, version: int):
        """
        Drop version of the model

        :param name: name of the model
        :param version: version to drop
        """
        ast_query = Delete(
            table=Identifier('models_versions'),
            where=dict_to_binary_op({
                'name': name,
                'version': version
            })
        )
        self.query(ast_query.to_string()).fetch()

    def list_jobs(self, name: str = None) -> List[Job]:
        """
        Show list of jobs in project

        :return: list of Job objects
        """

        ast_query = Select(targets=[Star()], from_table=Identifier('jobs'))

        if name is not None:
            ast_query.where = dict_to_binary_op({'name': name})

        df = self.api.sql_query(ast_query.to_string(), database=self.name)

        # columns to lower case
        cols_map = {i: i.lower() for i in df.columns}
        df = df.rename(columns=cols_map)

        return [
            Job(self, item)
            for item in df.to_dict('records')
        ]

    def get_job(self, name: str) -> Job:
        """
        Get job by name from project

        :param name: name of the job
        :return: Job object
        """

        jobs = self.list_jobs(name)
        if len(jobs) == 1:
            return jobs[0]
        elif len(jobs) == 0:
            raise AttributeError("Job doesn't exist")
        else:
            raise RuntimeError("Several jobs with the same name")

    def create_job(self, name: str, query_str:str,
                   start_at: dt.datetime = None, end_at: dt.datetime = None,
                   repeat_str: str = None) -> Union[Job, None]:
        """
        Create new job in project and return it.
        If it is not possible (job executed and not accessible anymore): return None
        More info: https://docs.mindsdb.com/sql/create/jobs

        :param name: name of the job
        :param query_str: str, job's query (or list of queries with ';' delimiter) which job have to execute
        :param start_at: datetime, first start of job,
        :param end_at: datetime, when job have to be stopped,
        :param repeat_str: str, optional, how to repeat job (e.g. '1 hour', '2 weeks', '3 min')
        :return: Job object or None
        """

        if start_at is not None:
            start_str = start_at.strftime("%Y-%m-%d %H:%M:%S")
        else:
            start_str = None

        if end_at is not None:
            end_str = end_at.strftime("%Y-%m-%d %H:%M:%S")
        else:
            end_str = None
        ast_query = CreateJob(
            name=Identifier(name),
            query_str=query_str,
            start_str=start_str,
            end_str=end_str,
            repeat_str=repeat_str
        )

        self.api.sql_query(ast_query.to_string(), database=self.name)

        # job can be executed and remove it is not repeatable
        jobs = self.list_jobs(name)
        if len(jobs) == 1:
            return jobs[0]

    def drop_job(self, name: str):
        """
        Drop job from project

        :param name: name of the job
        """
        ast_query = DropJob(Identifier(name))

        self.api.sql_query(ast_query.to_string(), database=self.name)


