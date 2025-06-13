import pytest

import datetime as dt
from unittest.mock import Mock
from unittest.mock import patch

import pandas as pd
from mindsdb_sql_parser import parse_sql

from mindsdb_sdk.models import ModelVersion, Model
from mindsdb_sdk.tables import Table
import mindsdb_sdk

from mindsdb_sdk.agents import Agent
from mindsdb_sdk.connect import DEFAULT_LOCAL_API_URL, DEFAULT_CLOUD_API_URL
from mindsdb_sdk.skills import SQLSkill
from mindsdb_sdk.connectors import rest_api

# patch _raise_for_status
rest_api._raise_for_status = Mock()


def response_mock(mock, data):
    if isinstance(data, pd.DataFrame):
        # to sql/query format (mostly used)
        pd_data = data.to_dict('split')
        data = {
            'type': 'table',
            'column_names': pd_data['columns'],
            'data': pd_data['data']
        }

    def side_effect(*args, **kwargs):
        r_mock = Mock()
        r_mock.status_code = 200
        r_mock.json.return_value = data
        return r_mock
    mock.side_effect = side_effect


def responses_mock(mock, data):
    side_effect_fns = []
    for d in data:
        if isinstance(d, pd.DataFrame):
            # to sql/query format (mostly used)
            pd_data = d.to_dict('split')
            d = {
                'type': 'table',
                'column_names': pd_data['columns'],
                'data': pd_data['data']
            }
        def side_effect(*args, **kwargs):
            r_mock = Mock()
            r_mock.status_code = 200
            r_mock.json.return_value = d
            return r_mock
        side_effect_fns.append(side_effect())
    mock.side_effect = side_effect_fns

def check_sql_call(mock, sql, database=None, call_stack_num=None):
    if call_stack_num is not None:
        call_args = mock.mock_calls[call_stack_num]
        args = call_args[1]
        kwargs = call_args[2]

    else:
        call_args = mock.call_args
        args = call_args[0]
        kwargs = call_args[1]

    assert args[0] == 'https://cloud.mindsdb.com/api/sql/query'
    sql_out = kwargs['json']['query']

    # re-render
    sql2 = parse_sql(sql, dialect='mindsdb').to_string()
    sql_out2 = parse_sql(sql_out, dialect='mindsdb').to_string()

    if sql_out not in (sql, sql2) and sql_out2 not in (sql, sql2):
        raise AssertionError(f'{sql} != {sql_out}')

    if database is not None:
        assert database == kwargs['json']['context']['db']


class BaseFlow:
    @patch('requests.Session.post')
    def check_model(self, model, database, mock_post):

        # using dataframe on input
        data_in = [{ 'a': 1 }]
        df_in = pd.DataFrame(data_in)

        data_out = [{ 'z': 2 }]
        response_mock(mock_post, data_out)

        params = { 'x': '1' }
        pred_df = model.predict(df_in, params=params)

        model_name = model.name
        if isinstance(model, ModelVersion):
            model_name = f'{model_name}.{model.version}'

        call_args = mock_post.call_args
        assert call_args[0][
                   0] == f'https://cloud.mindsdb.com/api/projects/{model.project.name}/models/{model_name}/predict'
        assert call_args[1]['json']['data'] == data_in
        assert call_args[1]['json']['params'] == params

        # check prediction
        assert (pred_df == pd.DataFrame(data_out)).all().bool()

        # predict using dict
        pred_df = model.predict({ 'a': 1 })
        assert (pred_df == pd.DataFrame(data_out)).all().bool()

        # using  deferred query
        response_mock(mock_post, pd.DataFrame(data_out))  # will be used sql/query

        query = database.query('select a from t1')
        pred_df = model.predict(query, params={ 'x': '1' })

        check_sql_call(mock_post,
                       f'SELECT m.* FROM (SELECT * FROM {query.database} (select a from t1)) AS t JOIN {model.project.name}.{model_name} AS m USING x="1"')
        assert (pred_df == pd.DataFrame(data_out)).all().bool()

        # using table
        table0 = database.tables.tbl0
        pred_df = model.predict(table0)

        check_sql_call(mock_post,
                       f'SELECT m.* FROM (SELECT * FROM {table0.db.name}.tbl0) AS t JOIN {model.project.name}.{model_name} AS m')
        assert (pred_df == pd.DataFrame(data_out)).all().bool()


        # time series prediction
        query = database.query('select * from t1 where type="house" and saledate>latest')
        model.predict(query)

        check_sql_call(mock_post,
                       f'SELECT m.* FROM (SELECT * FROM {query.database} (select * from t1 where type="house" and saledate>latest)) as t JOIN {model.project.name}.{model_name} AS m')
        assert (pred_df == pd.DataFrame(data_out)).all().bool()

        # -----------  model managing  --------------
        response_mock(
            mock_post,
            pd.DataFrame([{ 'NAME': 'm1', 'VERSION': 2, 'STATUS': 'complete' }])
        )

        model.finetune(query, options={ 'x': 2 })
        check_sql_call(
            mock_post,
            f'Finetune {model.project.name}.{model_name} FROM {query.database} ({query.sql})  USING x=2'
        )

        model.finetune('select a from t1', database='d1')
        check_sql_call(
            mock_post,
            f'Finetune {model.project.name}.{model_name} FROM d1 (select a from t1)'
        )

        model.retrain(query, options={'x': 2})
        check_sql_call(
            mock_post,
            f'RETRAIN {model.project.name}.{model_name} FROM {query.database} ({query.sql})  USING x=2'
        )

        model.retrain('select a from t1', database='d1', engine='openai')
        check_sql_call(
            mock_post,
            f'RETRAIN {model.project.name}.{model_name} FROM d1 (select a from t1) USING engine=\'openai\''
        )

        # describe
        if not isinstance(model, ModelVersion):  # not working (DESCRIBE db1.m1.2.ensemble not parsed)

            info = model.describe()  # dataframe on json. need to discuss
            check_sql_call(mock_post, f'DESCRIBE {model.project.name}.{model_name}')

            info = model.describe('ensemble')  # dataframe on json. need to discuss
            check_sql_call(mock_post, f'DESCRIBE {model.project.name}.{model_name}.ensemble')

        # -----------  versions  --------------

        # list all versions
        models = model.list_versions()
        check_sql_call(mock_post, f"SELECT * FROM models WHERE NAME = '{model.name}'",
                       database=model.project.name)
        model2 = models[0]  # Model object

        model2 = model.get_version(2)

        # change active version
        model2.set_active(version=3)

        # get call before last call
        mock_call = mock_post.call_args_list[-2]
        assert mock_call[1]['json']['query'] == f"SET active {model2.project.name}.{model2.name}.`3`"

    @patch('requests.Session.post')
    def check_table(self, table, mock_post):
        response_mock(mock_post, pd.DataFrame([{'x': 'a'}]))

        table2 = table.filter(a=3, b='2').limit(3)
        table2.fetch()
        str(table2)
        check_sql_call(mock_post, f'SELECT * FROM {table2.db.name}.{table2.name} WHERE a = 3 AND b = \'2\' LIMIT 3')

        # last
        table2 = table.filter(a=3).track('type')
        table2.fetch()
        check_sql_call(mock_post, f'SELECT * FROM {table2.db.name}.{table2.name} WHERE a = 3 AND type > last')


class Test(BaseFlow):

    @patch('requests.Session.get')
    @patch('requests.Session.put')
    @patch('requests.Session.post')
    def test_flow(self, mock_post, mock_put, mock_get):

        # check local
        server = mindsdb_sdk.connect()
        str(server)

        assert server.api.url == 'http://127.0.0.1:47334'

        # check cloud login
        server = mindsdb_sdk.connect(login='a@b.com')

        call_args = mock_post.call_args
        assert call_args[0][0] == 'https://cloud.mindsdb.com/cloud/login'
        assert call_args[1]['json']['email'] == 'a@b.com'

        # server status
        server.status()
        call_args = mock_get.call_args
        assert call_args[0][0] == 'https://cloud.mindsdb.com/api/status'

        # --------- databases -------------
        response_mock(
            mock_post,
            pd.DataFrame(
                [
                    {
                        "NAME": "db1",
                        "ENGINE": "postgres",
                        "CONNECTION_DATA": {"host": "zoop"},
                    }
                ]
            ),
        )

        databases = server.list_databases()

        check_sql_call(
            mock_post,
            "select NAME, ENGINE, CONNECTION_DATA from information_schema.databases where TYPE='data'",
        )

        database = databases[0]
        str(database)
        assert database.name == 'db1'
        self.check_database(database)

        database = server.get_database('db1')
        self.check_database(database)

        database = server.create_database(
            'pg1',
            engine='postgres',
            connection_args={'host': 'localhost'}
        )
        check_sql_call(mock_post, 'CREATE DATABASE pg1 WITH ENGINE = "postgres", PARAMETERS = {"host": "localhost"}')

        self.check_database(database)

        server.drop_database('pg1-a')
        check_sql_call(mock_post, 'DROP DATABASE `pg1-a`')

        # --------- projects -------------
        projects = server.list_projects()
        check_sql_call(mock_post, "select NAME from information_schema.databases where TYPE='project'")

        project = projects[0]
        assert project.name == 'db1'
        self.check_project(project, database)

        project = server.get_project('db1')
        self.check_project(project, database)

        project = server.create_project('proj1')
        check_sql_call(
            mock_post, 'CREATE DATABASE proj1 WITH ENGINE = "mindsdb", PARAMETERS = {}')
        self.check_project(project, database)

        server.drop_project('proj1-1')
        check_sql_call(mock_post, 'DROP DATABASE `proj1-1`')

        # test upload file
        response_mock(mock_post, pd.DataFrame([{'NAME': 'files', 'ENGINE': 'file', 'CONNECTION_DATA': {'host': 'woop'}}]))
        database = server.get_database('files')
        # create file
        df = pd.DataFrame([{'s': '1'}, {'s': 'a'}])
        database.create_table('my_file', df)

        call_args = mock_put.call_args
        assert call_args[0][0] == 'https://cloud.mindsdb.com/api/files/my_file'
        assert call_args[1]['data']['name'] == 'my_file'
        assert 'file' in call_args[1]['files']

    @patch('requests.Session.post')
    def test_managed_login(self, mock_post):

        mindsdb_sdk.connect(
            'http://instance_url', login='a@b.com', password='test_pass', is_managed=True)

        # check login
        call_args = mock_post.call_args
        assert call_args[0][0] == 'http://instance_url/api/login'
        assert call_args[1]['json']['username'] == 'a@b.com'
        assert call_args[1]['json']['password'] == 'test_pass'

    def check_project(self, project, database):
        self.check_project_views( project, database)

        self.check_project_models(project, database)

        self.check_project_models_versions(project, database)

        self.check_project_jobs(project)

    @patch('requests.Session.get')
    @patch('requests.Session.post')
    def check_project_views(self, project, database, mock_post, mock_get):
        # -----------  views  --------------

        response_mock(mock_get, [
            {'name': 'v1', 'type': 'view'},
        ])

        views = project.list_views()
        view = views[0]  # View object

        assert view.name == 'v1'

        # view has the same behaviour as table
        self.check_table(view)

        # get existing
        view = project.get_view('v1')

        assert view.name == 'v1'
        self.check_table(view)

        # create
        view = project.create_view(
            'v2',
            database='example_db',  # optional, can also be database object
            sql='select * from t1'
        )
        check_sql_call(mock_post, 'CREATE VIEW v2 from example_db (select * from t1)')

        assert view.name == 'v2'
        self.check_table(view)

        # using query object
        view = project.create_view(
            'v2',
            sql=project.query('select * from t1')
        )
        check_sql_call(mock_post, f'CREATE VIEW v2 from {project.name} (select * from t1)')

        assert view.name == 'v2'
        self.check_table(view)

        # drop
        project.drop_view('v2')
        check_sql_call(mock_post, 'DROP VIEW v2')

        project.drop_view('v2-v')
        check_sql_call(mock_post, 'DROP VIEW `v2-v`')

    @patch('requests.Session.post')
    def check_project_models(self, project, database, mock_post):
        # -----------  models  --------------
        response_mock(
            mock_post,
            pd.DataFrame([{'NAME': 'm1', 'VERSION': 1, 'STATUS': 'complete'}])
        )

        models = project.list_models()
        model = models[0]  # Model object

        assert model.name == 'm1'
        assert model.get_status() == 'complete'

        self.check_model(model, database)

        model = project.get_model('m1')
        assert model.name == 'm1'
        self.check_model(model, database)

        # create, using params
        response_mock(
            mock_post,
            pd.DataFrame([{'NAME': 'm2', 'VERSION': 1, 'STATUS': 'complete'}])
        )
        model = project.create_model(
            'm2',
            predict='price',
            engine='lightwood',
            database='example_db',
            query='select * from t1',
            options={
                'module': 'LightGBM'
            },
            timeseries_options={
                'order': 'date',
                'group': ['a', 'b'],
                'window': 10,
                'horizon': 2
            }
        )
        check_sql_call(
            mock_post,
            f'CREATE PREDICTOR {project.name}.m2 FROM example_db (select * from t1) PREDICT price ORDER BY date GROUP BY a, b WINDOW 10 HORIZON 2 USING module="LightGBM", `engine`="lightwood"'
        )
        assert model.name == 'm2'
        model.wait_complete()
        self.check_model(model, database)

        # create, using deferred query.
        query = database.query('select * from t2')
        model = project.create_model(
            'm2',
            predict='price',
            query=query,
        )
        str(query)

        check_sql_call(
            mock_post,
            f'CREATE PREDICTOR {project.name}.m2 FROM {database.name} (select * from t2) PREDICT price'
        )

        assert model.name == 'm2'
        self.check_model(model, database)

        project.drop_model('m3-a')
        check_sql_call(mock_post, f'DROP PREDICTOR {project.name}.`m3-a`')

        # the old way of join model with table
        sql = '''
          SELECT m.saledate as date, m.ma as forecast
         FROM mindsdb.house_sales_model as m
         JOIN example_db.demo_data.house_sales as t
         WHERE t.saledate > LATEST AND t.type = 'house'
         AND t.bedrooms=2
         LIMIT 4;
        '''
        result_df = project.query(sql).fetch()

        check_sql_call(mock_post, sql)

        # check ts params
        with pytest.raises(AttributeError):
            project.create_model(
                'm2',
                predict='price',
                engine='lightwood',
                database='example_db',
                query='select * from t1',
                options={
                    'module': 'LightGBM'
                },
                timeseries_options={
                    'order': 'date',
                    'group1': ['a', 'b'],
                }
            )

    @patch('requests.Session.post')
    def check_project_models_versions(self, project, database, mock_post):
        # -----------  model version --------------
        response_mock(
            mock_post,
            pd.DataFrame([{'NAME': 'm1', 'VERSION': 2, 'STATUS': 'complete'}])
        )

        # list
        models = project.list_models(with_versions=True)
        model = models[0]
        assert isinstance(model, ModelVersion)

        assert model.name == 'm1'
        assert model.version == 2

        self.check_model(model, database)

        # get
        model = project.get_model('m1', version=1)

        assert model.name == 'm1'
        assert model.version == 2

        self.check_model(model, database)

        project.drop_model_version('m1', 1)
        check_sql_call(mock_post, f"DROP PREDICTOR m1.`1`")

    @patch('requests.Session.post')
    def check_database(self, database, mock_post):

        # test query
        sql = 'select * from tbl1'
        query = database.query(sql)
        assert query.sql == sql
        table0 = database.tables.tbl0

        result = pd.DataFrame([{'s': '1'}, {'s': 'a'}])
        response_mock(mock_post, result)

        data = query.fetch()

        check_sql_call(mock_post, sql)

        assert (data == result).all().bool()

        # test tables
        response_mock(mock_post, pd.DataFrame([{'name': 't1'}]))
        tables = database.list_tables()
        table = tables[0]

        self.check_table(table)

        table = database.get_table('t1')
        assert table.name == 't1'
        self.check_table(table)

        # create from query
        table2 = database.create_table('t2', table0)
        check_sql_call(mock_post, f'create table {database.name}.t2 (select * from {database.name}.tbl0)')

        assert table2.name == 't2'
        self.check_table(table2)

        # create from table
        table1 = database.get_table('t1')
        table1 = table1.filter(b=2)
        table3 = database.create_table('t3', table1)
        check_sql_call(mock_post, f'create table {database.name}.t3 (SELECT * FROM {table1.db.name}.t1 WHERE b = 2)')

        assert table3.name == 't3'
        self.check_table(table3)

        # drop table
        database.drop_table('t3')
        check_sql_call(mock_post, f'drop table {database.name}.t3')

    @patch('requests.Session.post')
    def check_project_jobs(self, project, mock_post):

        response_mock(mock_post, pd.DataFrame([{
            'NAME': 'job1',
            'QUERY': 'select 1',
            'start_at': None,
            'end_at': None,
            'next_run_at': None,
            'schedule_str': None,
        }]))

        jobs = project.list_jobs()

        check_sql_call(mock_post, "select * from jobs")

        job = jobs[0]
        assert job.name == 'job1'
        assert job.query_str == 'select 1'

        job.refresh()
        check_sql_call(
            mock_post,
            f"select * from jobs where name = 'job1'"
        )

        project.create_job(
            name='job2',
            query_str='retrain m1',
            repeat_str='1 min',
            start_at=dt.datetime(2025, 2, 5, 11, 22),
            end_at=dt.date(2030, 1, 2)
        )

        check_sql_call(
            mock_post,
            f"CREATE JOB job2 (retrain m1) START '2025-02-05 11:22:00' END '2030-01-02 00:00:00' EVERY 1 min",
            call_stack_num=-2
        )

        project.drop_job('job2')

        check_sql_call(
            mock_post,
            f"DROP JOB job2"
        )


class TestSimplify(BaseFlow):

    @patch('requests.Session.put')
    @patch('requests.Session.post')
    def test_flow(self, mock_post, mock_put):

        con = mindsdb_sdk.connect(login='a@b.com')

        # check login
        call_args = mock_post.call_args
        assert call_args[0][0] == 'https://cloud.mindsdb.com/cloud/login'
        assert call_args[1]['json']['email'] == 'a@b.com'

        # --------- databases -------------
        response_mock(mock_post, pd.DataFrame([{'NAME': 'db1', 'ENGINE': 'postgres', 'CONNECTION_DATA': {}}]))

        databases = con.databases.list()

        check_sql_call(mock_post, "select NAME, ENGINE, CONNECTION_DATA from information_schema.databases where TYPE='data'")

        database = databases[0]
        assert database.name == 'db1'
        self.check_database(database)

        database = con.databases.get('db1')
        database = con.databases.db1
        self.check_database(database)

        database = con.databases.create(
            'pg1',
            engine='postgres',
            connection_args={'host': 'localhost'}
        )
        check_sql_call(mock_post, 'CREATE DATABASE pg1 WITH ENGINE = "postgres", PARAMETERS = {"host": "localhost"}')

        self.check_database(database)

        con.databases.drop('pg1-a')
        check_sql_call(mock_post, 'DROP DATABASE `pg1-a`')

        # --------- projects -------------
        # connection is also default project, check it
        self.check_project(con, database)

        projects = con.projects.list()
        check_sql_call(mock_post, "select NAME from information_schema.databases where TYPE='project'")

        project = projects[0]
        assert project.name == 'db1'
        self.check_project(project, database)

        project = con.projects.get('db1')
        project = con.projects.db1
        self.check_project(project, database)

        project = con.projects.create('proj1')
        str(project)
        check_sql_call(
            mock_post, 'CREATE DATABASE proj1 WITH ENGINE = "mindsdb", PARAMETERS = {}')
        self.check_project(project, database)

        con.projects.drop('proj1-1')
        check_sql_call(mock_post, 'DROP DATABASE `proj1-1`')

        # test upload file
        response_mock(mock_post, pd.DataFrame([{'NAME': 'files', 'ENGINE': 'file', 'CONNECTION_DATA': {}}]))
        database = con.databases.files
        # create file
        df = pd.DataFrame([{'s': '1'}, {'s': 'a'}])
        database.tables.create('my_file', df)

        call_args = mock_put.call_args
        assert call_args[0][0] == 'https://cloud.mindsdb.com/api/files/my_file'
        assert call_args[1]['data']['name'] == 'my_file'
        assert 'file' in call_args[1]['files']

        # --------- handlers -------------
        # data
        response_mock(mock_post,
                      pd.DataFrame([{'NAME': 'mysql', 'TYPE': 'data', 'TITLE': 'MySQL',
                                     'DESCRIPTION': "MindsDB handler for MySQL",
                                     'CONNECTION_ARGS': {'a': 1}}]))

        handlers = con.data_handlers.list()

        check_sql_call(mock_post, "show handlers WHERE type = 'data'")

        handler = handlers[0]
        assert handler.name == 'mysql'
        assert handler.title == 'MySQL'

        _ = con.ml_handlers.get('mysql')
        _ = con.ml_handlers.mysql

        # ml
        response_mock(mock_post,
                      pd.DataFrame([{'NAME': 'openai', 'TYPE': 'ml', 'TITLE': 'OpenAI',
                                     'DESCRIPTION': "MindsDB handler for OpenAI",
                                     'CONNECTION_ARGS': {'a': 1}}]))

        handlers = con.ml_handlers.list()

        check_sql_call(mock_post, "show handlers WHERE type = 'ml'")

        handler = handlers[0]
        assert handler.name == 'openai'
        assert handler.title == 'OpenAI'

        _ = con.ml_handlers.get('openai')
        openai_handler = con.ml_handlers.openai

        # --------- ml_engines -------------
        response_mock(mock_post, pd.DataFrame([{ 'NAME': 'openai1', 'HANDLER': 'openai', 'CONNECTION_DATA': {'a': 1}}]))

        ml_engines = con.ml_engines.list()

        check_sql_call(mock_post, "show ml_engines")

        ml_engine = ml_engines[0]
        assert ml_engine.name == 'openai1'
        assert ml_engine.handler == 'openai'

        _ = con.ml_engines.get('openai1')
        _ = con.ml_engines.openai1

        con.ml_engines.create(
            'openai1',
            openai_handler,
            connection_data={'api_key': 'sk-11'}
        )
        check_sql_call(mock_post, 'CREATE ML_ENGINE openai1 FROM openai USING api_key = \'sk-11\'')

        con.ml_engines.create(
            'openai1',
            'openai',
            connection_data={'api_key': '111'}
        )
        check_sql_call(mock_post, 'CREATE ML_ENGINE openai1 FROM openai USING api_key = "111"')

        con.ml_engines.drop('openai1')
        check_sql_call(mock_post, 'DROP ML_ENGINE openai1')

        # byom
        model = '''
import pandas as pd

class CustomPredictor():

    def train(self, df, target_col, args=None):
        
        self.target_col=target_col 
        
     def predict(self, df):
        return pd.Dataframe([{'predict': self.target_col}])
'''
        requirements = '''pandas'''

        con.ml_engines.create_byom('b1', model, requirements)
        call_args = mock_put.call_args
        assert call_args[0][0] == 'https://cloud.mindsdb.com/api/handlers/byom/b1'
        assert call_args[1]['files']['code'] == model
        assert call_args[1]['files']['modules'] == requirements

    def check_project(self, project, database):
        self.check_project_views( project, database)

        model = self.check_project_models(project, database)

        self.check_project_models_versions(project, database)

        kb = self.check_project_kb(project, database)

        self.check_project_jobs(project, model, database, kb)

    @patch('requests.Session.get')
    @patch('requests.Session.post')
    def check_project_views(self, project, database, mock_post, mock_get):
        # -----------  views  --------------

        response_mock(mock_get, [
            {'name': 'v1', 'type': 'view'},
        ])

        views = project.views.list()
        view = views[0]  # View object

        assert view.name == 'v1'

        # view has the same behaviour as table
        self.check_table(view)

        # get existing
        view = project.views.get('v1')
        view = project.views.v1

        assert view.name == 'v1'
        self.check_table(view)

        # create
        view = project.views.create(
            'v2',
            database='example_db',  # optional, can also be database object
            sql='select * from t1'
        )
        check_sql_call(mock_post, 'CREATE VIEW v2 from example_db (select * from t1)')

        assert view.name == 'v2'
        self.check_table(view)

        # using query object
        view = project.views.create(
            'v2',
            sql=project.query('select * from t1')
        )
        check_sql_call(mock_post, f'CREATE VIEW v2 from {project.name} (select * from t1)')

        assert view.name == 'v2'
        self.check_table(view)

        # drop
        project.views.drop('v2')
        check_sql_call(mock_post, 'DROP VIEW v2')

        project.views.drop('v2-v')
        check_sql_call(mock_post, 'DROP VIEW `v2-v`')

    @patch('requests.Session.post')
    def check_project_models(self, project, database, mock_post):
        # -----------  models  --------------
        response_mock(
            mock_post,
            pd.DataFrame([{'NAME': 'm1', 'VERSION': 1, 'STATUS': 'complete'}])
        )

        models = project.models.list()
        model = models[0]  # Model object
        str(model)

        assert model.name == 'm1'
        assert model.get_status() == 'complete'

        self.check_model(model, database)

        model = project.models.get('m1')
        model = project.models.m1

        assert model.name == 'm1'
        self.check_model(model, database)

        # create, using params
        response_mock(
            mock_post,
            pd.DataFrame([{'NAME': 'm2', 'VERSION': 1, 'STATUS': 'complete'}])
        )
        model = project.models.create(
            'm2',
            predict='price',
            engine='lightwood',
            database='example_db',
            query='select * from t1',
            timeseries_options={
                'order': 'date',
                'group': ['a', 'b'],
                'window': 10,
                'horizon': 2
            },
            module='LightGBM',  # has to be in options
        )
        check_sql_call(
            mock_post,
            f'CREATE PREDICTOR {project.name}.m2 FROM example_db (select * from t1) PREDICT price ORDER BY date GROUP BY a, b WINDOW 10 HORIZON 2 USING module="LightGBM", `engine`="lightwood"'
        )
        assert model.name == 'm2'
        self.check_model(model, database)

        # create, using deferred query.
        query = database.query('select * from t2')
        model = project.models.create(
            'm2',
            predict='price',
            query=query,
        )

        check_sql_call(
            mock_post,
            f'CREATE PREDICTOR {project.name}.m2 FROM {database.name} (select * from t2) PREDICT price'
        )

        # create without database
        model = project.models.create(
            'm2',
            predict='response',
            engine='openai',
            options={'prompt': 'make up response'},
        )

        check_sql_call(
            mock_post,
            f'CREATE PREDICTOR {project.name}.m2 PREDICT response USING prompt="make up response", `engine`="openai"'
        )

        assert model.name == 'm2'
        self.check_model(model, database)

        project.models.drop('m3-a')
        check_sql_call(mock_post, f'DROP PREDICTOR {project.name}.`m3-a`')

        # the old way of join model with table
        sql = '''
          SELECT m.saledate as date, m.ma as forecast
         FROM mindsdb.house_sales_model as m
         JOIN example_db.demo_data.house_sales as t
         WHERE t.saledate > LATEST AND t.type = 'house'
         AND t.bedrooms=2
         LIMIT 4;
        '''
        result_df = project.query(sql).fetch()

        check_sql_call(mock_post, sql)

        # check ts params
        with pytest.raises(AttributeError):
            project.models.create(
                'm2',
                predict='price',
                engine='lightwood',
                database='example_db',
                query='select * from t1',
                options={
                    'module': 'LightGBM'
                },
                timeseries_options={
                    'order': 'date',
                    'group1': ['a', 'b'],
                }
            )

        return model

    @patch('requests.Session.post')
    def check_project_models_versions(self, project, database, mock_post):
        # -----------  model version --------------
        response_mock(
            mock_post,
            pd.DataFrame([{'NAME': 'm1', 'VERSION': 2, 'STATUS': 'complete'}])
        )

        # list
        models = project.models.list(with_versions=True)
        model = models[0]
        assert isinstance(model, ModelVersion)

        assert model.name == 'm1'
        assert model.version == 2

        self.check_model(model, database)

        # get
        model = project.models.get('m1', version=1)

        assert model.name == 'm1'
        assert model.version == 2

        self.check_model(model, database)

        project.models.m1.drop_version(1)
        check_sql_call(mock_post, f"DROP PREDICTOR m1.`1`")

    @patch('requests.Session.post')
    def check_database(self, database, mock_post):

        # test query
        sql = 'select * from tbl1'
        query = database.query(sql)
        assert query.sql == sql

        table0 = database.tables.tbl0

        result = pd.DataFrame([{'s': '1'}, {'s': 'a'}])
        response_mock(mock_post, result)

        data = query.fetch()

        check_sql_call(mock_post, sql)

        assert (data == result).all().bool()

        # test tables
        response_mock(mock_post, pd.DataFrame([{'name': 't1'}]))
        tables = database.tables.list()
        table = tables[0]

        self.check_table(table)

        table = database.tables.get('t1')
        table = database.tables.t1
        assert table.name == 't1'
        self.check_table(table)

        # create from query
        table2 = database.tables.create('t2', table0)
        check_sql_call(mock_post, f'create table {database.name}.t2 (select * from {database.name}.tbl0)')

        # create with replace
        database.tables.create('t2', table0, replace=True)
        check_sql_call(mock_post, f'create or replace table {database.name}.t2 (select * from {database.name}.tbl0)')


        assert table2.name == 't2'
        self.check_table(table2)

        # -- insert into table --
        # from dataframe
        table2.insert(pd.DataFrame([{'s': '1', 'x': 1}, {'s': 'a', 'x': 2}]))
        check_sql_call(mock_post, f"INSERT INTO {table2.db.name}.t2(s, x) VALUES ('1', 1), ('a', 2)")

        # from query
        table2.insert(query)
        check_sql_call(mock_post, f"INSERT INTO {database.name}.t2 (select * from {query.database}(select * from tbl1))")

        # -- delete in table --
        table2.delete(a=1, b='2')
        check_sql_call(mock_post, f"DELETE FROM {database.name}.t2 WHERE a = 1 AND b = '2'")

        # -- update table --
        # from query
        table2.update(query, on=['a', 'b'])
        check_sql_call(mock_post, f"UPDATE {database.name}.t2 ON a, b FROM (select * from  {query.database}(select * from tbl1))")

        # from dict
        table2.update({'a': '1', 'b': 1}, filters={'x': 3})
        check_sql_call(mock_post, f"UPDATE {table2.db.name}.t2 SET a='1', b=1 WHERE x=3")

        # create from table
        table1 = database.tables.t1
        table1 = table1.filter(b=2)
        table3 = database.tables.create('t3', table1)
        check_sql_call(mock_post, f'create table {database.name}.t3 (SELECT * FROM {table1.db.name}.t1 WHERE b = 2)')

        assert table3.name == 't3'
        self.check_table(table3)

        # drop table
        database.tables.drop('t3')
        check_sql_call(mock_post, f'drop table {database.name}.t3')

    @patch('requests.Session.post')
    def check_project_jobs(self, project, model, database, kb, mock_post):

        response_mock(mock_post, pd.DataFrame([{
            'NAME': 'job1',
            'QUERY': 'select 1',
            'start_at': None,
            'end_at': None,
            'next_run_at': None,
            'schedule_str': None,
        }]))

        jobs = project.jobs.list()

        check_sql_call(mock_post, "select * from jobs")

        job = jobs[0]
        assert job.name == 'job1'
        assert job.query_str == 'select 1'

        dir(project.jobs)
        job = project.jobs.job1
        str(job)
        assert job.name == 'job1'
        assert job.query_str == 'select 1'

        job.refresh()
        check_sql_call(
            mock_post,
            f"select * from jobs where name = 'job1'"
        )

        job.get_history()

        check_sql_call(
            mock_post,
            f"select * from jobs_history where name = 'job1'"
        )

        project.jobs.create(
            name='job2',
            query_str='retrain m1',
            repeat_min=1,
            start_at=dt.datetime(2025, 2, 5, 11, 22),
            end_at=dt.date(2030, 1, 2)
        )

        check_sql_call(
            mock_post,
            f"CREATE JOB job2 (retrain m1) START '2025-02-05 11:22:00' END '2030-01-02 00:00:00' EVERY 1 minutes",
            call_stack_num=-2
        )

        project.jobs.create(
            name='job2',
            query_str='retrain m1'
        )

        check_sql_call(
            mock_post,
            f"CREATE JOB job2 (retrain m1)",
            call_stack_num=-2
        )

        project.jobs.drop('job2')

        check_sql_call(
            mock_post,
            f"DROP JOB job2"
        )

        # using context
        with project.jobs.create(name='job2', repeat_min=1) as job:
            job.add_query(model.retrain())
            job.add_query(model.predict(database.tables.tbl1))
            job.add_query(kb.insert(database.tables.tbl1))
            job.add_query('show models')

        retrain_sql = f'RETRAIN {model.project.name}.{model.name}'
        predict_sql = f'SELECT m.* FROM (SELECT * FROM {database.name}.tbl1) AS t JOIN {model.project.name}.{model.name} AS m'
        kb_sql = f'INSERT INTO {kb.project.name}.{kb.name} (SELECT * FROM {database.name}.tbl1)'

        check_sql_call(
            mock_post,
            f"CREATE JOB job2 ({retrain_sql}; {predict_sql}; {kb_sql}; show models) EVERY 1 minutes",
            call_stack_num=-2
        )

    @patch('requests.Session.put')
    @patch('requests.Session.post')
    @patch('requests.Session.delete')
    @patch('requests.Session.get')
    def check_project_kb(self, project, database, mock_get, mock_del, mock_post, mock_put):

        response_mock(mock_post, pd.DataFrame([{
            'NAME': 'my_kb',
            'PROJECT': 'mindsdb',
            'EMBEDDING_MODEL': {
                'PROVIDER': 'openai',
                'MODEL_NAME': 'openai_emb',
                'API_KEY': 'sk-...'
            },
            'RERANKING_MODEL': {
                'PROVIDER': 'openai',
                'MODEL_NAME': 'openai_rerank',
                'API_KEY': 'sk-...'
            },
            'STORAGE': 'pvec.tbl1',
            'PARAMS': {"id_column": "num"},
        }]))

        example_kb = {
            'id': 1,
            'name': 'my_kb',
            'project_id': 1,
            'embedding_model': {
                'provider': 'openai',
                'model_name': 'openai_emb',
                'api_key': 'sk-...'
            },
            'reranking_model': {
                'provider': 'openai',
                'model_name': 'openai_rerank',
                'api_key': 'sk-...'
            },
            'vector_database': 'pvec',
            'vector_database_table': 'tbl1',
            'updated_at': '2024-10-04 10:55:25.350799',
            'created_at': '2024-10-04 10:55:25.350790',
            'params': {}
        }

        mock_get().json.return_value = [example_kb]

        kbs = project.knowledge_bases.list()

        args, kwargs = mock_get.call_args
        assert args[0] == f'{DEFAULT_CLOUD_API_URL}/api/projects/{project.name}/knowledge_bases'

        kb = kbs[0]

        assert kb.name == 'my_kb'

        assert kb.embedding_model['model_name'] == 'openai_emb'
        assert kb.reranking_model['model_name'] == 'openai_rerank'

        assert isinstance(kb.storage, Table)
        assert kb.storage.name == 'tbl1'
        assert kb.storage.db.name == 'pvec'

        mock_get().json.return_value = example_kb
        kb = project.knowledge_bases.my_kb

        str(kb)
        assert kb.name == 'my_kb'
        assert kb.storage.db.name == 'pvec'
        assert kb.embedding_model['model_name'] == 'openai_emb'
        assert kb.reranking_model['model_name'] == 'openai_rerank'

        # --- insert ---

        # table
        kb.insert(
            database.tables.tbl2.filter(a=1)
        )

        args, kwargs = mock_put.call_args
        assert args[0] == f'{DEFAULT_CLOUD_API_URL}/api/projects/{project.name}/knowledge_bases/my_kb'
        assert kwargs == {'json': {'knowledge_base': {'query': 'SELECT * FROM pg1.tbl2 WHERE a = 1'}}}

        # query
        kb.insert(
            database.query('select * from tbl2 limit 1')
        )
        args, kwargs = mock_put.call_args
        assert args[0] == f'{DEFAULT_CLOUD_API_URL}/api/projects/{project.name}/knowledge_bases/my_kb'
        assert kwargs == {'json': {'knowledge_base': {'query': 'select * from tbl2 limit 1'}}}

        # dataframe
        kb.insert(
            pd.DataFrame([[1, 'Alice'], [2, 'Bob']], columns=['id', 'name'])
        )

        args, kwargs = mock_put.call_args
        assert args[0] == f'{DEFAULT_CLOUD_API_URL}/api/projects/{project.name}/knowledge_bases/my_kb'
        assert kwargs == {'json': {
            'knowledge_base': {'rows': [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}]}
        }}

        # query
        df = kb.find(query='dog', limit=5).fetch()

        check_sql_call(
            mock_post,
            f'''select * from {project.name}.{kb.name} where content='dog' limit 5'''
        )

        # create 1
        project.knowledge_bases.create(
            name='kb2',
            embedding_model={
                'provider': 'openai',
                'model_name': 'openai_emb',
                'api_key': 'sk-...'
            },
            reranking_model={
                'provider': 'openai',
                'model_name': 'openai_rerank',
                'api_key': 'sk-...'
            },
            metadata_columns=['date', 'author'],
            params={'k': 'v'}
        )
        args, kwargs = mock_post.call_args
        assert args[0] == f'{DEFAULT_CLOUD_API_URL}/api/projects/{project.name}/knowledge_bases'
        assert kwargs == {'json': {'knowledge_base': {
            'name': 'kb2',
            'embedding_model': {
                'provider': 'openai',
                'model_name': 'openai_emb',
                'api_key': 'sk-...'
            },
            'reranking_model': {
                'provider': 'openai',
                'model_name': 'openai_rerank',
                'api_key': 'sk-...'
            },
            'metadata_columns': ['date', 'author'],
            'params': {
                'k': 'v',
            }
        }}}

        # create 2
        kb = project.knowledge_bases.create(
            name='kb2',
            storage=database.tables.tbl1,
            content_columns=['review'],
            id_column='num'
        )

        args, kwargs = mock_post.call_args
        assert args[0] == f'{DEFAULT_CLOUD_API_URL}/api/projects/{project.name}/knowledge_bases'
        assert kwargs == {'json': {'knowledge_base': {
            'name': 'kb2',
            'content_columns': ['review'],
            'id_column': 'num',
            'storage': {
                'database': database.name,
                'table': 'tbl1'
            },
        }}}

        # completion
        kb.completion('hi', type='chat', llm_model='gpt-4')
        args, kwargs = mock_post.call_args
        assert args[0] == f'{DEFAULT_CLOUD_API_URL}/api/projects/{project.name}/knowledge_bases/{kb.name}/completions'
        assert kwargs == {'json': {
            'query': 'hi',
            'type': 'chat',
            'llm_model': 'gpt-4',
        }}

        # drop
        project.knowledge_bases.drop('kb2')

        args, kwargs = mock_del.call_args
        assert args[0] == f'{DEFAULT_CLOUD_API_URL}/api/projects/{project.name}/knowledge_bases/kb2'

        return kb


class TestAgents():
    @patch('requests.Session.get')
    def test_list(self, mock_get):
        response_mock(mock_get, [])
        server = mindsdb_sdk.connect()
        assert len(server.agents.list()) == 0

        created_at = dt.datetime(2000, 3, 1, 9, 30)
        updated_at = dt.datetime(2001, 3, 1, 9, 30)
        response_mock(mock_get, [
            {
                'id': 1,
                'name': 'test_agent',
                'project_id': 1,
                'model_name': 'test_model',
                'skills': [],
                'params': {},
                'created_at': created_at,
                'updated_at': updated_at,
                'provider': 'mindsdb'
            }
        ])
        all_agents = server.agents.list()
        # Check API call.
        assert mock_get.call_args[0][0] == f'{DEFAULT_LOCAL_API_URL}/api/projects/mindsdb/agents'

        assert len(all_agents) == 1
        expected_agent = Agent(
            'test_agent',
            'test_model',
            [],
            {},
            created_at,
            updated_at,
            'mindsdb'
        )
        assert all_agents[0] == expected_agent

    @patch('requests.Session.get')
    def test_get(self, mock_get):
        server = mindsdb_sdk.connect()
        created_at = dt.datetime(2000, 3, 1, 9, 30)
        updated_at = dt.datetime(2001, 3, 1, 9, 30)
        response_mock(mock_get,
            {
                'id': 1,
                'name': 'test_agent',
                'project_id': 1,
                'model_name': 'test_model',
                'skills': [],
                'params': {},
                'created_at': created_at,
                'updated_at': updated_at,
                'provider': 'mindsdb'
            }
        )
        agent = server.agents.get('test_agent')
        # Check API call.
        assert mock_get.call_args[0][0] == f'{DEFAULT_LOCAL_API_URL}/api/projects/mindsdb/agents/test_agent'
        expected_agent = Agent(
            'test_agent',
            'test_model',
            [],
            {},
            created_at,
            updated_at,
            'mindsdb'
        )
        assert agent == expected_agent

    @patch('requests.Session.post')
    @patch('requests.Session.get')
    def test_create(self, mock_get, mock_post):
        created_at = dt.datetime(2000, 3, 1, 9, 30)
        updated_at = dt.datetime(2001, 3, 1, 9, 30)
        data = {
            'id': 1,
            'name': 'test_agent',
            'project_id': 1,
            'model_name': 'test_model',
            'skills': [{
                'id': 0,
                'name': 'test_skill',
                'project_id': 1,
                'type': 'sql',
                'params': {'tables': ['test_table'], 'database': 'test_database', 'description': 'test_description'},
            }],
            'params': {'k1': 'v1'},
            'created_at': created_at,
            'updated_at': updated_at,
            'provider': 'mindsdb',
        }
        responses_mock(mock_post, [
            # ML Engine get (SQL post for SHOW ML_ENGINES)
            data
        ])
        responses_mock(mock_get, [
            # Skill get.
            {'name': 'test_skill', 'type': 'sql', 'params': {'tables': ['test_table'], 'database': 'test_database', 'description': 'test_description'}},
        ])

        # Create the agent.
        server = mindsdb_sdk.connect()
        new_agent = server.agents.create(
            name='test_agent',
            model=Model(None, {'name':'m1'}),
            skills=['test_skill'],
            params={'k1': 'v1'}
        )
        # Check API call.
        assert len(mock_post.call_args_list) == 1
        assert mock_post.call_args_list[-1][0][0] == f'{DEFAULT_LOCAL_API_URL}/api/projects/mindsdb/agents'
        assert mock_post.call_args_list[-1][1]['json'] == {
            'agent': {
                'name': 'test_agent',
                'model_name': 'm1',
                'skills': ['test_skill'],
                'params': {
                    'k1': 'v1',
                    'prompt_template': 'Answer the user"s question in a helpful way: {{question}}'
                },
                'provider': 'mindsdb'
            }
        }
        expected_skill = SQLSkill('test_skill', ['test_table'], 'test_database', 'test_description')
        expected_agent = Agent(
            'test_agent',
            'test_model',
            [expected_skill],
            {'k1': 'v1'},
            created_at,
            updated_at,
            'mindsdb'
        )

        assert new_agent == expected_agent

    @patch('requests.Session.get')
    @patch('requests.Session.put')
    # Mock creating new skills.
    @patch('requests.Session.post')
    def test_update(self, mock_get, mock_put, _):
        created_at = dt.datetime(2000, 3, 1, 9, 30)
        updated_at = dt.datetime(2001, 3, 1, 9, 30)
        data = {
            'id': 1,
            'name': 'test_agent',
            'project_id': 1,
            'model_name': 'updated_model',
            'skills': [{
                'id': 1,
                'name': 'updated_skill',
                'project_id': 1,
                'type': 'sql',
                'params': {'tables': ['updated_table'], 'database': 'updated_database', 'description': 'test_description'},
            }],
            'params': {'k2': 'v2'},
            'created_at': created_at,
            'updated_at': updated_at,
            'provider': 'mindsdb',
        }
        response_mock(mock_put, data)

        # Mock existing agent.
        response_mock(mock_get, {
            'id': 1,
            'name': 'test_agent',
            'project_id': 1,
            'model_name': 'test_model',
            'skills': [],
            'params': {'k1': 'v1'},
            'provider': 'mindsdb',
        })

        server = mindsdb_sdk.connect()
        expected_agent = Agent(
            'test_agent',
            'updated_model',
            [SQLSkill('updated_skill', ['updated_table'], 'updated_database', 'test_description')],
            {'k2': 'v2'},
            created_at,
            updated_at,
            'mindsdb'
        )

        updated_agent = server.agents.update('test_agent', expected_agent)
        # Check API calls.
        assert mock_put.call_args[0][0] == f'{DEFAULT_LOCAL_API_URL}/api/projects/mindsdb/agents/test_agent'
        assert mock_put.call_args[1]['json'] == {
            'agent': {
                'name': 'test_agent',
                'model_name': 'updated_model',
                'skills_to_add': ['updated_skill'],
                'skills_to_remove': [],
                'params': {'k2': 'v2'},
                'provider': 'mindsdb'
            }
        }

        assert updated_agent == expected_agent

    @patch('requests.Session.post')
    def test_completion(self, mock_post):
        response_mock(mock_post, {
            'message':
                {
                    'content': 'Angel Falls in Venezuela at 979m',
                    'role': 'assistant',
                }
        })
        server = mindsdb_sdk.connect()
        messages = [{
            'question': 'What is the highest waterfall in the world?',
            'answer': None
        }]
        completion = server.agents.completion('test_agent', messages)
        # Check API call.
        assert mock_post.call_args[0][0] == f'{DEFAULT_LOCAL_API_URL}/api/projects/mindsdb/agents/test_agent/completions'
        assert mock_post.call_args[1]['json'] == {
           'messages': messages
        }
        assert completion.content == 'Angel Falls in Venezuela at 979m'

    @patch('requests.Session.delete')
    def test_delete(self, mock_delete):
        server = mindsdb_sdk.connect()
        server.agents.drop('test_agent')
        # Check API call.
        assert mock_delete.call_args[0][0] == f'{DEFAULT_LOCAL_API_URL}/api/projects/mindsdb/agents/test_agent'

    @patch('requests.Session.get')
    @patch('requests.Session.put')
    @patch('requests.Session.post')
    def test_add_file(self, mock_post, mock_put, mock_get):
        server = mindsdb_sdk.connect()
        responses_mock(mock_get, [
            # File metadata get.
            [{'name': 'tokaido_rules'}],
            # Existing agent get.
            {
                'name': 'test_agent',
                'model_name': 'test_model',
                'skills': [],
                'params': {},
                'created_at': None,
                'updated_at': None,
                'provider': 'mindsdb'
            },
            # get KB
            {
                'id': 1,
                'name': 'my_kb',
                'project_id': 1,
                'embedding_model': 'openai_emb',
                'vector_database': 'pvec',
                'vector_database_table': 'tbl1',
                'updated_at': '2024-10-04 10:55:25.350799',
                'created_at': '2024-10-04 10:55:25.350790',
                'params': {}
            },
            # Skills get in Agent update to check if it exists.
            {'name': 'new_skill', 'type': 'retrieval', 'params': {'source': 'test_agent_tokaido_rules_kb'}},
            # Existing agent get in Agent update.
            {
                'name': 'test_agent',
                'model_name': 'test_model',
                'skills': [],
                'params': {},
                'created_at': None,
                'updated_at': None,
                'provider': 'mindsdb'
            },
        ])
        responses_mock(mock_post, [
            # Skill creation.
            {'name': 'new_skill', 'type': 'retrieval', 'params': {'source': 'test_agent_tokaido_rules_kb'}}
        ])
        responses_mock(mock_put, [
            # KB update.
            {'name': 'test_agent_tokaido_rules_kb'},
            # Agent update with new skill.
            {
                'name': 'test_agent',
                'model_name': 'test_model',
                'skills': [{'name': 'new_skill', 'type': 'retrieval', 'params': {'source': 'test_agent_tokaido_rules_kb'}}],
                'params': {},
                'created_at': None,
                'updated_at': None,
                'provider': 'mindsdb'
            },
        ])
        server.agents.add_file('test_agent', './tokaido_rules.pdf', 'Rules for the board game Tokaido', 'existing_kb')

        # Check Agent was updated with a new skill.
        agent_update_json = mock_put.call_args[-1]['json']
        expected_agent_json = {
            'agent': {
                'name': 'test_agent',
                'model_name': 'test_model',
                # Skill name is a generated UUID.
                'skills_to_add': [agent_update_json['agent']['skills_to_add'][0]],
                'skills_to_remove': [],
                'params': {},
                'provider': 'mindsdb'
            }
        }
        assert agent_update_json == expected_agent_json

    @patch('requests.Session.get')
    @patch('requests.Session.put')
    @patch('requests.Session.post')
    def test_add_webpage(self, mock_post, mock_put, mock_get):
        server = mindsdb_sdk.connect()
        responses_mock(mock_get, [
            # Existing agent get.
            {
                'name':'test_agent',
                'model_name':'test_model',
                'skills':[],
                'params':{},
                'created_at':None,
                'updated_at':None,
                'provider':'mindsdb'
            },
            # get KB
            {
                'id': 1,
                'name': 'my_kb',
                'project_id': 1,
                'embedding_model': 'openai_emb',
                'vector_database': 'pvec',
                'vector_database_table': 'tbl1',
                'updated_at': '2024-10-04 10:55:25.350799',
                'created_at': '2024-10-04 10:55:25.350790',
                'params': {}
            },
            # Skills get in Agent update to check if it exists.
            {'name':'new_skill', 'type':'retrieval', 'params':{'source':'test_agent_docs_mdb_ai_kb'}},
            # Existing agent get in Agent update.
            {
                'name':'test_agent',
                'model_name':'test_model',
                'skills':[],
                'params':{},
                'created_at':None,
                'updated_at':None,
                'provider':'mindsdb'  # Added provider field
            },
        ])
        responses_mock(mock_post, [
            # Skill creation.
            {'name':'new_skill', 'type':'retrieval', 'params':{'source':'test_agent_docs_mdb_ai_kb'}}
        ])
        responses_mock(mock_put, [
            # KB update.
            {'name':'test_agent_docs_mdb_ai_kb'},
            # Agent update with new skill.
            {
                'name':'test_agent',
                'model_name':'test_model',
                'skills':[{'name':'new_skill', 'type':'retrieval', 'params':{'source':'test_agent_docs_mdb_ai_kb'}}],
                'params':{},
                'created_at':None,
                'updated_at':None,
                'provider':'mindsdb'  # Added provider field
            },
        ])
        server.agents.add_webpage('test_agent', 'docs.mdb.ai', 'Documentation for MindsDB', 'existing_kb')

        # Check Agent was updated with a new skill.
        agent_update_json = mock_put.call_args[-1]['json']
        expected_agent_json = {
            'agent':{
                'name':'test_agent',
                'model_name':'test_model',
                # Skill name is a generated UUID.
                'skills_to_add':[agent_update_json['agent']['skills_to_add'][0]],
                'skills_to_remove':[],
                'params':{},
                'provider': 'mindsdb'
            }
        }
        assert agent_update_json == expected_agent_json

    @patch('requests.Session.get')
    @patch('requests.Session.put')
    @patch('requests.Session.post')
    def test_add_database(self, mock_post, mock_put, mock_get):
        server = mindsdb_sdk.connect()
        responses_mock(mock_get, [
            # Existing agent get.
            {
                'name': 'test_agent',
                'model_name': 'test_model',
                'skills': [],
                'params': {},
                'created_at': None,
                'updated_at': None,
                'provider': 'mindsdb'
            },
            # Skills get in Agent update to check if it exists.
            {'name': 'new_skill', 'type': 'sql', 'params': {'database': 'existing_db', 'tables': ['existing_table']}},
            # Existing agent get in Agent update.
            {
                'name': 'test_agent',
                'model_name': 'test_model',
                'skills': [],
                'params': {},
                'created_at': None,
                'updated_at': None,
                'provider': 'mindsdb'
            },
        ])
        responses_mock(
            mock_post,
            [
                # DB get (POST /sql).
                pd.DataFrame(
                    [
                        {
                            "NAME": "existing_db",
                            "ENGINE": "postgres",
                            "CONNECTION_DATA": {"host": "boop"},
                        }
                    ]
                ),
                # DB tables get (POST /sql).
                pd.DataFrame([{"name": "existing_table"}]),
                # Skill creation.
                {
                    "name": "new_skill",
                    "type": "sql",
                    "params": {"database": "existing_db", "tables": ["existing_table"]},
                },
            ],
        )
        responses_mock(mock_put, [
            # Agent update with new skill.
            {
                'name': 'test_agent',
                'model_name': 'test_model',
                'skills': [{'name': 'new_skill', 'type': 'sql', 'params': {'database': 'existing_db', 'tables': ['existing_table']}}],
                'params': {},
                'created_at': None,
                'updated_at': None,
                'provider': 'mindsdb'
            },
        ])
        server.agents.add_database('test_agent', 'existing_db', ['existing_table'], 'My data')

        # Check Agent was updated with a new skill.
        agent_update_json = mock_put.call_args[-1]['json']
        expected_agent_json = {
            'agent': {
                'name': 'test_agent',
                'model_name': 'test_model',
                # Skill name is a generated UUID.
                'skills_to_add': [agent_update_json['agent']['skills_to_add'][0]],
                'skills_to_remove': [],
                'params': {'prompt_template': 'using mindsdb sqltoolbox'},
                'provider': 'mindsdb'
            }
        }
        assert agent_update_json == expected_agent_json

class TestSkills():
    @patch('requests.Session.get')
    def test_list(self, mock_get):
        response_mock(mock_get, [])
        server = mindsdb_sdk.connect()
        # Check API call.
        assert len(server.skills.list()) == 0
        assert mock_get.call_args[0][0] == f'{DEFAULT_LOCAL_API_URL}/api/projects/mindsdb/skills'

        created_at = dt.datetime(2000, 3, 1, 9, 30)
        updated_at = dt.datetime(2001, 3, 1, 9, 30)
        response_mock(mock_get, [
            {
                'id': 1,
                'name': 'test_skill',
                'project_id': 1,
                'params': {'tables': ['test_table'], 'database': 'test_database', 'description': 'test_description' },
                'type': 'sql'
            }
        ])
        all_skills = server.skills.list()
        assert len(all_skills) == 1

        expected_skill = SQLSkill('test_skill', ['test_table'], 'test_database', 'test_description')
        assert all_skills[0] == expected_skill

    @patch('requests.Session.get')
    def test_get(self, mock_get):
        server = mindsdb_sdk.connect()
        response_mock(mock_get,
            {
                'id': 1,
                'name': 'test_skill',
                'project_id': 1,
                'params': {'tables': ['test_table'], 'database': 'test_database', 'description': 'test_description'},
                'type': 'sql'
            }
        )
        skill = server.skills.get('test_skill')
        # Check API call.
        assert mock_get.call_args[0][0] == f'{DEFAULT_LOCAL_API_URL}/api/projects/mindsdb/skills/test_skill'
        expected_skill = SQLSkill('test_skill', ['test_table'], 'test_database', 'test_description')
        assert skill == expected_skill

    @patch('requests.Session.post')
    def test_create(self, mock_post):
        data = {
            'id': 1,
            'name': 'test_skill',
            'project_id': 1,
            'params': {'tables': ['test_table'], 'database': 'test_database', 'description': 'test_description'},
            'type': 'test'
        }
        response_mock(mock_post, data)

        # Create the skill.
        server = mindsdb_sdk.connect()
        new_skill = server.skills.create(
            'test_skill',
            'sql',
            params={'tables': ['test_table'], 'database': 'test_database', 'description': 'test_description'}
        )
        # Check API call.
        assert mock_post.call_args[0][0] == f'{DEFAULT_LOCAL_API_URL}/api/projects/mindsdb/skills'
        assert mock_post.call_args[1]['json'] == {
           'skill': {
                'name': 'test_skill',
                'type': 'sql',
                'params': {'database': 'test_database', 'tables': ['test_table'], 'description': 'test_description'}
            }
        }
        expected_skill = SQLSkill('test_skill', ['test_table'], 'test_database', 'test_description')

        assert new_skill == expected_skill

    @patch('requests.Session.put')
    def test_update(self, mock_put):
        data = {
            'id': 1,
            'name': 'test_skill',
            'project_id': 1,
            'params': {'tables': ['updated_table'], 'database': 'updated_database', 'description': 'updated_description'},
            'type': 'sql'
        }
        response_mock(mock_put, data)

        server = mindsdb_sdk.connect()
        expected_skill = SQLSkill('test_skill', ['updated_table'], 'updated_database', 'updated_description')

        updated_skill = server.skills.update('test_skill', expected_skill)
        # Check API call.
        assert mock_put.call_args[0][0] == f'{DEFAULT_LOCAL_API_URL}/api/projects/mindsdb/skills/test_skill'
        assert mock_put.call_args[1]['json'] == {
           'skill': {
                'name': 'test_skill',
                'type': 'sql',
                'params': {'tables': ['updated_table'], 'database': 'updated_database', 'description': 'updated_description'}
            }
        }

        assert updated_skill == expected_skill

    @patch('requests.Session.delete')
    def test_delete(self, mock_delete):
        server = mindsdb_sdk.connect()
        server.skills.drop('test_skill')
        # Check API call.
        assert mock_delete.call_args[0][0] == f'{DEFAULT_LOCAL_API_URL}/api/projects/mindsdb/skills/test_skill'
