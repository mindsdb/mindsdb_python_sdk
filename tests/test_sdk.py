import pytest

import datetime as dt
from unittest.mock import Mock
from unittest.mock import patch

import pandas as pd
from mindsdb_sql import parse_sql

from mindsdb_sdk.model import ModelVersion
import mindsdb_sdk

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
                       f'SELECT m.* FROM (SELECT a FROM t1) as t JOIN {model.project.name}.{model_name} AS m USING x="1"')
        assert (pred_df == pd.DataFrame(data_out)).all().bool()

        # time series prediction
        query = database.query('select * from t1 where type="house" and saledate>latest')
        model.predict(query)

        check_sql_call(mock_post,
                       f"SELECT m.* FROM (SELECT * FROM t1 WHERE (type = 'house') AND (saledate > LATEST)) as t JOIN {model.project.name}.{model_name} AS m")
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

        model.retrain(query, options={ 'x': 2 })
        check_sql_call(
            mock_post,
            f'RETRAIN {model.project.name}.{model_name} FROM {query.database} ({query.sql})  USING x=2'
        )

        model.retrain('select a from t1', database='d1')
        check_sql_call(
            mock_post,
            f'RETRAIN {model.project.name}.{model_name} FROM d1 (select a from t1)'
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
        check_sql_call(mock_post, f"SELECT * FROM models_versions WHERE NAME = '{model.name}'",
                       database=model.project.name)
        model2 = models[0]  # Model object

        model2 = model.get_version(2)

        # change active version
        model2.set_active(version=3)

        # get call before last call
        mock_call = mock_post.call_args_list[-2]
        assert mock_call[1]['json'][
                   'query'] == f"update models_versions set active=1 where (name = '{model2.name}') AND (version = 3)"

    @patch('requests.Session.post')
    def check_table(self, table, mock_post):
        response_mock(mock_post, pd.DataFrame([{'x': 'a'}]))

        table = table.filter(a=3, b='2')
        table = table.limit(3)
        table.fetch()

        check_sql_call(mock_post, f'SELECT * FROM {table.name} WHERE (a = 3) AND (b = \'2\') LIMIT 3')


class Test(BaseFlow):

    @patch('requests.Session.put')
    @patch('requests.Session.post')
    def test_flow(self, mock_post, mock_put):

        server = mindsdb_sdk.connect(login='a@b.com')

        # check login
        call_args = mock_post.call_args
        assert call_args[0][0] == 'https://cloud.mindsdb.com/cloud/login'
        assert call_args[1]['json']['email'] == 'a@b.com'

        # --------- databases -------------
        response_mock(mock_post, pd.DataFrame([{'NAME': 'db1'}]))

        databases = server.list_databases()

        check_sql_call(mock_post, "select NAME from information_schema.databases where TYPE='data'")

        database = databases[0]
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
        response_mock(mock_post, pd.DataFrame([{'NAME': 'files'}]))
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
            f'CREATE PREDICTOR m2 FROM example_db (select * from t1) PREDICT price ORDER BY date GROUP BY a, b WINDOW 10 HORIZON 2 USING module="LightGBM", `engine`="lightwood"'
        )
        assert model.name == 'm2'
        self.check_model(model, database)

        # create, using deferred query.
        query = database.query('select * from t2')
        model = project.create_model(
            'm2',
            predict='price',
            query=query,
        )

        check_sql_call(
            mock_post,
            f'CREATE PREDICTOR m2 FROM {database.name} (select * from t2) PREDICT price'
        )

        assert model.name == 'm2'
        self.check_model(model, database)

        project.drop_model('m3-a')
        check_sql_call(mock_post, f'DROP PREDICTOR `m3-a`')

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
        # TODO
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
        check_sql_call(mock_post, f"delete from models_versions where name='m1' and version=1")


    @patch('requests.Session.post')
    def check_database(self, database, mock_post):

        # test query
        sql = 'select * from tbl1'
        query = database.query(sql)
        assert query.sql == sql

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
        table2 = database.create_table('t2', query)
        check_sql_call(mock_post, f'create table {database.name}.t2 (select * from tbl1)')

        assert table2.name == 't2'
        self.check_table(table2)

        # create from table
        table1 = database.get_table('t1')
        table1 = table1.filter(b=2)
        table3 = database.create_table('t3', table1)
        check_sql_call(mock_post, f'create table {database.name}.t3 (SELECT * FROM t1 WHERE b = 2)')

        assert table3.name == 't3'
        self.check_table(table3)

        # drop table
        database.drop_table('t3')
        check_sql_call(mock_post, f'drop table t3')


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
        response_mock(mock_post, pd.DataFrame([{'NAME': 'db1'}]))

        databases = con.databases.list()

        check_sql_call(mock_post, "select NAME from information_schema.databases where TYPE='data'")

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
        check_sql_call(
            mock_post, 'CREATE DATABASE proj1 WITH ENGINE = "mindsdb", PARAMETERS = {}')
        self.check_project(project, database)

        con.projects.drop('proj1-1')
        check_sql_call(mock_post, 'DROP DATABASE `proj1-1`')

        # test upload file
        response_mock(mock_post, pd.DataFrame([{'NAME': 'files'}]))
        database = con.databases.files
        # create file
        df = pd.DataFrame([{'s': '1'}, {'s': 'a'}])
        database.tables.create('my_file', df)

        call_args = mock_put.call_args
        assert call_args[0][0] == 'https://cloud.mindsdb.com/api/files/my_file'
        assert call_args[1]['data']['name'] == 'my_file'
        assert 'file' in call_args[1]['files']

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
            module = 'LightGBM',  # has to be in options
        )
        check_sql_call(
            mock_post,
            f'CREATE PREDICTOR m2 FROM example_db (select * from t1) PREDICT price ORDER BY date GROUP BY a, b WINDOW 10 HORIZON 2 USING module="LightGBM", `engine`="lightwood"'
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
            f'CREATE PREDICTOR m2 FROM {database.name} (select * from t2) PREDICT price'
        )

        assert model.name == 'm2'
        self.check_model(model, database)

        project.models.drop('m3-a')
        check_sql_call(mock_post, f'DROP PREDICTOR `m3-a`')

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
        # TODO
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
        check_sql_call(mock_post, f"delete from models_versions where name='m1' and version=1")

    @patch('requests.Session.post')
    def check_database(self, database, mock_post):

        # test query
        sql = 'select * from tbl1'
        query = database.query(sql)
        assert query.sql == sql

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
        table2 = database.tables.create('t2', query)
        check_sql_call(mock_post, f'create table {database.name}.t2 (select * from tbl1)')

        assert table2.name == 't2'
        self.check_table(table2)

        # create from table
        table1 = database.tables.t1
        table1 = table1.filter(b=2)
        table3 = database.tables.create('t3', table1)
        check_sql_call(mock_post, f'create table {database.name}.t3 (SELECT * FROM t1 WHERE b = 2)')

        assert table3.name == 't3'
        self.check_table(table3)

        # drop table
        database.tables.drop('t3')
        check_sql_call(mock_post, f'drop table t3')



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

        jobs = project.jobs.list()

        check_sql_call(mock_post, "select * from jobs")

        job = jobs[0]
        assert job.name == 'job1'
        assert job.query_str == 'select 1'

        job = project.jobs.job1
        assert job.name == 'job1'
        assert job.query_str == 'select 1'

        job.refresh()
        check_sql_call(
            mock_post,
            f"select * from jobs where name = 'job1'"
        )

        project.jobs.create(
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

        project.jobs.drop('job2')

        check_sql_call(
            mock_post,
            f"DROP JOB job2"
        )

