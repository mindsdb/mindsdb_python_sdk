from functools import wraps
import io

import requests
import pandas as pd

from .. import __about__

def _try_relogin(fnc):
    @wraps(fnc)
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


def _raise_for_status(response):
    # show response text in error
    if 400 <= response.status_code < 600:
        raise requests.HTTPError(f'{response.reason}: {response.text}', response=response)


class RestAPI:
    def __init__(self, url=None, login=None, password=None, is_managed=False):

        self.url = url
        self.username = login
        self.password = password
        self.is_managed = is_managed
        self.session = requests.Session()

        self.session.headers['User-Agent'] = f'python-sdk/{__about__.__version__}'

        if login is not None:
            self.login()

    def login(self):
        managed_endpoint = '/api/login'
        cloud_endpoint = '/cloud/login'

        if self.is_managed:
            json = {'password': self.password, 'username': self.username}
            url = self.url + managed_endpoint
        else:
            json = {'password': self.password, 'email': self.username}
            url = self.url + cloud_endpoint
        r = self.session.post(url, json=json)

        # failback when is using managed instance with is_managed=False
        if r.status_code in (405, 404) and self.is_managed is False:
            # try managed instance login

            json = {'password': self.password, 'username': self.username}
            url = self.url + managed_endpoint
            r = self.session.post(url, json=json)

        _raise_for_status(r)

    @_try_relogin
    def sql_query(self, sql, database='mindsdb', lowercase_columns=False):
        url = self.url + '/api/sql/query'
        r = self.session.post(url, json={
            'query': sql,
            'context': {'db': database}
        })
        _raise_for_status(r)

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
        _raise_for_status(r)

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
        _raise_for_status(r)

        return pd.DataFrame(r.json())

    @_try_relogin
    def objects_tree(self, item=''):
        r = self.session.get(self.url + f'/api/tree/{item}')
        _raise_for_status(r)

        return pd.DataFrame(r.json())

    @_try_relogin
    def upload_file(self, name: str, df: pd.DataFrame):

        # convert to file
        fd = io.BytesIO()
        df.to_csv(fd)
        fd.seek(0)

        url = self.url + f'/api/files/{name}'
        r = self.session.put(
            url,
            data={
                'source': name,
                'name': name,
                'source_type': 'file',
            },
            files={
                'file': fd,
            }
        )
        _raise_for_status(r)