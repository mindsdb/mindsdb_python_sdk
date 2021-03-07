import os
import requests

from mindsdb_sdk.classes import authorizers


def for_status_raiser(func):
    """Decorator which raises exception if response code returned by 'func'
    is not 200"""
    def wrapper(*args, **kwargs):
        r = func(*args, **kwargs)
        try:
            r.raise_for_status()
        except Exception as e:
            raise Exception(f"Error with message: {r.text}") from e
        return r.json()
    return wrapper


class Proxy:

    def __init__(self, host, user=None, password=None, token=None) -> None:
        self._host = host.rstrip('/')
        self._apikey = None

        if (user is not None and password is not None) or token is not None:
            self._authorizer = authorizers.CloudAuthorizer(host, user, password, token=token)
        else:
            self._authorizer = authorizers.BaseAuthorizer(host, user, password, token=token)

    @for_status_raiser
    def post(self, route, data=None, json=None, params=None):
        if params is None:
            params = {}

        return self._authorizer('post',
                                self._host + '/api' + route,
                                data=data,
                                params=params,
                                json=json)

    @for_status_raiser
    def put(self, route, data=None, json=None, params=None, files=None, params_processing=True):

        if params_processing:
            if params is None:
                params = {}

            if files is not None:
                del_tmp = False
                if 'df' in files:
                    del_tmp = True
                    files['df'].to_csv('tmp_upload_file.csv', index=False)
                    files['file'] = 'tmp_upload_file.csv'
                    del files['df']

                with open(files['file'], 'rb') as fp:
                    files['file'] = fp
                    data = {}
                    data['source_type'] = 'file'

                    response = self._authorizer('put',
                                                self._host + '/api' + route,
                                                files=files,
                                                data=data)

                if del_tmp:
                    os.remove('tmp_upload_file.csv')

                return response


        response = self._authorizer('put',
                                    self._host + '/api' + route,
                                    data=data,
                                    params=params,
                                    json=json,
                                    files=files)


        return response

    @for_status_raiser
    def get(self, route, params=None):
        if params is None:
            params = {}

        return self._authorizer('get',
                                self._host + '/api' + route,
                                params=params)


    @for_status_raiser
    def delete(self, route, params=None):
        if params is None:
            params = {}

        return self._authorizer('delete',
                                self._host + '/api' + route,
                                params=params)


    def ping(self):
        try:
            return self.get('/util/ping')['status'] == 'ok'
        except Exception as _:
            return False
