import requests


class BaseAuthorizer:
    def __init__(self, host, username, password, *args, **kwargs):
        self.host = host
        self.username = username
        self.password = password
        self.token = kwargs.get('token', None)

    def __call__(self, req_type, url,  **kwargs):
        return getattr(requests, req_type)(url, **kwargs)


class CloudAuthorizer(BaseAuthorizer):
    def __init__(self, host, username, password, *args, **kwargs):
        super().__init__(host, username, password, *args, **kwargs)
        self.base_url = f"{self.host}/cloud"
        self.token = self._get_api_token()

    def _get_api_token(self):
        if self.token is not None:
            return self.token
        token_url = self.base_url + '/login'
        json = {'email': self.username, 'password': self.password}
        r = requests.post(token_url, json=json)
        r.raise_for_status()

        if 'apiKey' in r.cookies:
            return r.cookies['apiKey']
        else:
            return r.cookies['session']
        
    @property
    def auth_cookies(self):
        return {
            'apiKey': self.token,
            'session': self.token,
            'confirmed': '1'
        }


    def __call__(self, req_type, url,  **kwargs):
        kwargs['cookies'] = self.auth_cookies
        return getattr(requests, req_type)(url, **kwargs)


class UrlTokenAuthorizer(BaseAuthorizer):
    def __init__(self, host, username, password, url_token):
        super().__init__(host, username, password)
        self.token = url_token

    def __call__(self, req_type, url,  **kwargs):
        params = kwargs.get('params', {})
        params['apikey'] = self.token
        kwargs['params'] = params
        return getattr(requests, req_type)(url, **kwargs)
