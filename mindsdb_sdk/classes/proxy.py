import requests
from requests.compat import urljoin
from pathlib import Path

class Proxy(object):

    def __init__(self, host, user=None, password=None, token=None) -> None:
        self._host = host.rstrip('/')
        self._apikey = None

        if token is not None:
            self._apikey = token
        if user is not None:
            self._apikey = self.post('/api/login', json={'email': user, 'password': password})['token']


    def post(self, route, data=None, json=None, params=None):
        if params is None:
            params = {}
        if self._apikey is not None:
            params['apiKey'] = self._apikey
        
        if data is not None:
            response = requests.post(self._host + route, data=data, params=params)
        elif json is not None:
            response = requests.post(self._host + route, json=data, params=params)
        else:
            response = requests.post(self._host + route, params=params)
        
        return response.json()

    def put(self, route, data=None, json=None, params=None):
        if params is None:
            params = {}
        if self._apikey is not None:
            params['apiKey'] = self._apikey
        
        if data is not None:
            response = requests.put(self._host + route, data=data, params=params)
        elif json is not None:
            response = requests.put(self._host + route, json=data, params=params)
        else:
            response = requests.put(self._host + route, params=params)
        
        return response.json()

    def get(self, route, params=None):
        if params is None:
            params = {}
        if self._apikey is not None:
            params['apiKey'] = self._apikey

        response = requests.get(self._host + route, params=params)
        
        return response.json()

    def delete(self, url: str):
        if params is None:
            params = {}
        if self._apikey is not None:
            params['apiKey'] = self._apikey

        response = requests.get(self._host + route, params=params)
        
        return response.json()

    def ping(self):
        try:
            return self.get('/util/ping')['status'] == 'ok'
        except Exception as e:
            return False

