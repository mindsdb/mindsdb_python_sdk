from functools import wraps

import requests
import pandas as pd


def _try_relogin(fnc):
    wraps(fnc)
    def wrapper(self, *args, **kwargs):
        try:
            return fnc(self, *args, **kwargs)
        except requests.HTTPError as e:
            if e.response.status_code != 401:
                raise e

            # try re-login
            try:
                self.login()
            except requests.HTTPError:
                raise e
            # call once more
            return fnc(self, *args, **kwargs)
    return wrapper


class RestAPI:
    def __init__(self, url=None, email=None, password=None):

        self.url = url
        self.email = email
        self.password = password
        self.session = requests.Session()

        if email is not None:
            self.login()

    def login(self):
        url = self.url + '/cloud/login'
        json = {'email': self.email, 'password': self.password}
        r = self.session.post(url, json=json)
        r.raise_for_status()

    @_try_relogin
    def sql_query(self, sql, database=None, lowercase_columns=False):
        if database is None:
            database = 'mindsdb'
        url = self.url + '/api/sql/query'
        r = self.session.post(url, json={
            'query': sql,
            'context': {'db': database}
        })
        r.raise_for_status()

        data = r.json()
        if data['type'] == 'table':
            columns = data['column_names']
            if lowercase_columns:
                columns = [i.lower() for i in columns]
            return pd.DataFrame(data['data'], columns=columns)
        if data['type'] == 'error':
            raise RuntimeError(data['error_message'])
        return None

    @_try_relogin
    def projects(self):
        # TODO not used yet

        r = self.session.get(self.url + '/api/projects')
        r.raise_for_status()

        return pd.DataFrame(r.json())

    @_try_relogin
    def model_predict(self, project, model, data, params=None, version=None):
        data = data.to_dict('records')

        if version is not None:
            model = f'{model}.{version}'
        if params is None:
            params = {}
        url = self.url + f'/api/projects/{project}/models/{model}/predict'
        r = self.session.post(url, json={
            'data': data,
            'params': params
        })
        r.raise_for_status()

        return pd.DataFrame(r.json())

    @_try_relogin
    def objects_tree(self, item=''):
        r = self.session.get(self.url + f'/api/tree/{item}')
        r.raise_for_status()

        return pd.DataFrame(r.json())
