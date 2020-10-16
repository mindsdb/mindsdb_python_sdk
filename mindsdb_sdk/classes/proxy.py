import requests
from requests.compat import urljoin
from pathlib import Path
from typing import Optional, List, Tuple

class Proxy(object):
    _server: Optional[str] = None
    _apikey: Optional[str] = None
    timeout: int = 120

    def __init__(self, server: str, apikey: Optional[str] = None) -> None:
        self._server = server
        self._apikey = apikey

    def login(self, email: str, password: str) -> None:
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
    def _apikeyParam(self) -> dict:
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

    def _is_success(self, response) -> bool:
        return response.status_code == 200

    def ping(self) -> bool:
        r = self._get('/util/ping')
        return self._is_success(r)

    def get_datasources(self) -> List['DataSource']:
        r = self._get('/datasources')
        if self._is_success(r):
            return r.json()
        return []

    def put_datasource(self, name: str, path: str) -> bool:
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

    def put_datasource_by_url(self, name: str, url: str) -> bool:
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

    def delete_datasource(self, name: str) -> bool:
        r = self._delete(f"/datasources/{name}")
        return self._is_success(r)

    def analyze_datasource(self, name: str) -> dict:
        r = self._get(f"/datasources/{name}/analyze")
        return r.json()

    def get_datasource_data(self, name: str) -> dict:
        r = self._get(f"/datasources/{name}/data")
        return r.json()

    def get_datasource_file(self, name: str) -> Tuple[bytes, str]:
        r = self._get(f"/datasources/{name}/download")
        filename = r.headers['Content-Disposition'].split('filename=')[1]
        content = r.content
        return content, filename

    def get_predictors(self) -> List:
        r = self._get('/predictors')
        if self._is_success(r):
            return r.json()
        return []

    def learn_predictor(self, predictor_name: str, data_source_name: str, to_predict: List[str]) -> bool:
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

    def delete_predictor(self, predictor_name: str) -> bool:
        r = self._delete(f"/predictors/{predictor_name}")
        return self._is_success(r)

    def predict(self, predictor_name: str, when: dict) -> dict:
        r = requests.post(
            url=urljoin(self._server, f"/predictors/{predictor_name}/predict"),
            params=self._apikeyParam,
            json={'when': when},
            timeout=self.timeout,
            allow_redirects=True
        )
        return r.json()

    def download_predictor(self, predictor_name: str) -> bytes:
        p = self._get(f'/predictors/{predictor_name}/download')
        return p.content

    def upload_predictor(self, file_path: str) -> bool:
        files = { 'file': open(file_path, 'rb') }
        r = requests.put(
            url=urljoin(self._server, '/predictors/upload'),
            params=self._apikeyParam,
            files=files,
            timeout=self.timeout,
            allow_redirects=True
        )
        return self._is_success(r)
