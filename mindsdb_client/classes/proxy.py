import requests
from requests.compat import urljoin
from pathlib import Path

class Proxy(object):
    _server = None
    _apikey = None
    timeout = 120

    def __init__(self, server, apikey=None):
        self._server = server
        self._apikey = apikey

    def login(self, email, password):
        response = requests.post(
            url=urljoin(self._server, '/api/login'),
            json={'email': email, 'password': password},
            timeout=self.timeout,
            allow_redirects=True
        )
        if self._is_success(response):
            self._apikey = response.json()['token']
            return

    @property
    def _apikeyParam(self):
        return {'apikey': self._apikey}
    
    def _get(self, url: str):
        return requests.get(
            url=urljoin(self._server, url),
            params=self._apikeyParam,
            timeout=self.timeout,
            allow_redirects=True
        )

    def _delete(self, url: str):
        return requests.delete(
            url=urljoin(self._server, url),
            params=self._apikeyParam,
            timeout=self.timeout,
            allow_redirects=True
        )

    def _is_success(self, response):
        return response.status_code == 200

    def ping(self):
        r = self._get('/util/ping')
        return self._is_success(r)

    def get_datasources(self):
        r = self._get('/datasources')
        if self._is_success(r):
            return r.json()
        return []

    def put_datasource(self, name, path):
        file = Path(path)
        if file.is_file() is False:
            raise Exception('wrong file path')
        data = {
            'name': name,
            'source_type': 'file',
            'source': file.name
        }
        files = { 'file': open(path, 'rb') }
        r = requests.put(
            url=urljoin(self._server, f"/datasources/{data['name']}"),
            params=self._apikeyParam,
            data=data,
            files=files,
            timeout=self.timeout,
            allow_redirects=True
        )
        return self._is_success(r)

    def put_datasource_by_url(self, name, url):
        data = {
            'name': name,
            'source_type': 'url',
            'source': url
        }
        r = requests.put(
            url=urljoin(self._server, f"/datasources/{data['name']}"),
            json=data,
            params=self._apikeyParam,
            timeout=self.timeout,
            allow_redirects=True
        )
        return self._is_success(r)

    def delete_datasource(self, name):
        r = self._delete(f"/datasources/{name}")
        return self._is_success(r)

    def get_predictors(self):
        r = self._get('/predictors')
        if self._is_success(r):
            return r.json()
        return []

    def learn_predictor(self, predictor_name, data_source_name, to_predict):
        r = requests.put(
            url=urljoin(self._server, f"/predictors/{predictor_name}"),
            params=self._apikeyParam,
            json={
                'data_source_name': data_source_name,
                'to_predict': to_predict
            },
            timeout=self.timeout,
            allow_redirects=True
        )
        return self._is_success(r)

    def delete_predictor(self, predictor_name):
        r = self._delete(f"/predictors/{predictor_name}")
        return self._is_success(r)

    def predict(self, predictor_name, when):
        r = requests.post(
            url=urljoin(self._server, f"/predictors/{predictor_name}/predict"),
            params=self._apikeyParam,
            json={'when': when},
            timeout=self.timeout,
            allow_redirects=True
        )
        return r.json()
