from functools import wraps
from typing import List, Union
import io

import requests
import pandas as pd

from mindsdb_sdk import __about__


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
    def __init__(self, url=None, login=None, password=None, api_key=None, is_managed=False, headers=None):

        self.url = url
        self.username = login
        self.password = password
        self.api_key = api_key
        self.is_managed = is_managed
        self.session = requests.Session()

        self.session.headers['User-Agent'] = f'python-sdk/{__about__.__version__}'
        if headers is not None:
            self.session.headers.update(headers)
        if self.api_key is not None:
            # Authenticate with API key instead of logging in, if present.
            self.session.headers['X-Api-Key'] = self.api_key
            return
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
    def sql_query(self, sql, database=None, lowercase_columns=False):

        if database is None:
            # it means the database is included in query
            database = 'mindsdb'
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

    @staticmethod
    def read_file_as_bytes(file_path: str):
        """
        Read and return content of a file in bytes, given its path.
        :param file_path: Path of the file to read.
        :return: File content in bytes.
        """
        try:
            with open(file_path, 'rb+') as file:
                return file.read()
        except FileNotFoundError:
            raise Exception(f'File {file_path} does not exist.')
        except PermissionError:
            raise Exception(f'Permission denied when reading file {file_path}.')
        except Exception as e:
            raise Exception(f'Unknown error occurred when reading file {file_path} - {str(e)}')
    @staticmethod
    def read_dataframe_as_csv(data: pd.DataFrame):
        """
        Read and return content of a DataFrame as CSV in bytes.
        :param data: DataFrame to read.
        :return: DataFrame content as CSV in bytes.
        """
        fd = io.BytesIO()
        data.to_csv(fd, index=False)
        fd.seek(0)
        return fd.read()

    def upload_data(self, file_name: str, data: bytes):
        """
        Upload binary data to MindsDB.
        :param file_name: Name of the file.
        :param data: Binary data to upload.
        """
        # remove suffix from file if present
        name = file_name.split('.')[0]

        url = self.url + f'/api/files/{name}'
        r = self.session.put(
            url,
            data={
                'original_file_name':file_name,
                'name':name,
                'source_type':'file',
            },
            files={
                'file': (file_name, data)

            }
        )
        _raise_for_status(r)

    @_try_relogin
    def upload_file(self, name: str, data: Union[pd.DataFrame, str]):
        """
        Upload a file or a DataFrame to MindsDB.
        :param name: Name of the file or DataFrame.
        :param data: DataFrame data or file path.
        """
        if isinstance(data, pd.DataFrame):
            data_in_bytes = self.read_dataframe_as_csv(data)
        else:
            data_in_bytes = self.read_file_as_bytes(data)

        self.upload_data(name, data_in_bytes)

    @_try_relogin
    def get_file_metadata(self, name: str) -> dict:
        # No endpoint currently to get single file.
        url = self.url + f'/api/files/'
        r = self.session.get(url)
        _raise_for_status(r)
        all_file_metadata = r.json()
        for metadata in all_file_metadata:
            if metadata.get('name', None) == name:
                return metadata
        r.status_code = 404
        raise requests.HTTPError(f'Not found: No file named {name} found', response=r)

    @_try_relogin
    def upload_byom(self, name: str, code: str, requirements: str):

        url = self.url + f'/api/handlers/byom/{name}'
        r = self.session.put(
            url,
            files={
                'code': code,
                'modules': requirements,
            }
        )
        _raise_for_status(r)

    def status(self) -> dict:

        r = self.session.get(self.url + '/api/status')
        _raise_for_status(r)

        return r.json()

    # TODO: Different endpoints should be refactored into their own classes.
    #
    # Agents operations.
    @_try_relogin
    def agents(self, project: str):
        r = self.session.get(self.url + f'/api/projects/{project}/agents')
        _raise_for_status(r)

        return r.json()

    @_try_relogin
    def agent(self, project: str, name: str):
        r = self.session.get(self.url + f'/api/projects/{project}/agents/{name}')
        _raise_for_status(r)

        return r.json()

    @_try_relogin
    def agent_completion(self, project: str, name: str, messages: List[dict]):
        url = self.url + f'/api/projects/{project}/agents/{name}/completions'
        r = self.session.post(
            url,
            json={
                'messages': messages
            }
        )
        _raise_for_status(r)

        return r.json()

    @_try_relogin
    def create_agent(self, project: str, name: str, model: str, skills: List[str] = None, params: dict = None):
        url = self.url + f'/api/projects/{project}/agents'
        r = self.session.post(
            url,
            json={
                'agent': {
                    'name': name,
                    'model_name': model,
                    'skills': skills,
                    'params': params
                }
            }
        )
        _raise_for_status(r)
        return r.json()

    @_try_relogin
    def update_agent(
            self,
            project: str,
            name: str,
            updated_name: str,
            updated_model: str,
            skills_to_add: List[str],
            skills_to_remove: List[str],
            updated_params: dict
            ):
        url = self.url + f'/api/projects/{project}/agents/{name}'
        r = self.session.put(
            url,
            json={
                'agent': {
                    'name': updated_name,
                    'model_name': updated_model,
                    'skills_to_add': skills_to_add,
                    'skills_to_remove': skills_to_remove,
                    'params': updated_params
                }
            }
        )
        _raise_for_status(r)
        return r.json()

    @_try_relogin
    def delete_agent(self, project: str, name: str):
        url = self.url + f'/api/projects/{project}/agents/{name}'
        r = self.session.delete(url)
        _raise_for_status(r)

    # Skills operations.
    @_try_relogin
    def skills(self, project: str):
        r = self.session.get(self.url + f'/api/projects/{project}/skills')
        _raise_for_status(r)

        return r.json()

    @_try_relogin
    def skill(self, project: str, name: str):
        r = self.session.get(self.url + f'/api/projects/{project}/skills/{name}')
        _raise_for_status(r)

        return r.json()

    @_try_relogin
    def create_skill(self, project: str, name: str, type: str, params: dict):
        url = self.url + f'/api/projects/{project}/skills'
        r = self.session.post(
            url,
            json={
               'skill': {
                    'name': name,
                    'type': type,
                    'params': params
                }
            }
        )
        _raise_for_status(r)

        return r.json()

    @_try_relogin
    def update_skill(
            self,
            project: str,
            name: str,
            updated_name: str,
            updated_type: str,
            updated_params: dict
            ):
        url = self.url + f'/api/projects/{project}/skills/{name}'
        r = self.session.put(
            url,
            json={
               'skill': {
                    'name': updated_name,
                    'type': updated_type,
                    'params': updated_params
                }
            }
        )
        _raise_for_status(r)
        return r.json()

    @_try_relogin
    def delete_skill(self, project: str, name: str):
        url = self.url + f'/api/projects/{project}/skills/{name}'
        r = self.session.delete(url)
        _raise_for_status(r)

    # Knowledge Base operations.
    @_try_relogin
    def insert_files_into_knowledge_base(self, project: str, knowledge_base_name: str, file_names: List[str]):
        r = self.session.put(
            self.url + f'/api/projects/{project}/knowledge_bases/{knowledge_base_name}',
            json={
                'knowledge_base': {
                    'files': file_names
                }
            }
        )
        _raise_for_status(r)

        return r.json()

    @_try_relogin
    def insert_webpages_into_knowledge_base(self, project: str, knowledge_base_name: str, urls: List[str]):
        r = self.session.put(
            self.url + f'/api/projects/{project}/knowledge_bases/{knowledge_base_name}',
            json={
                'knowledge_base': {
                    'urls': urls
                }
            }
        )
        _raise_for_status(r)

        return r.json()
