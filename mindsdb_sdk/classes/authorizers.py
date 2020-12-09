import requests


class BaseAuthorizer:
    def __init__(self, host, username, password, *args, **kwargs):
        self.host = host
        self.username = username
        self.password = password
        self.token = kwargs.get('token', None)

    @property
    def auth_cookies(self):
        cookies = {}
        if self.token:
            cookies.update({'apiKey': self.token})
        return cookies


class CloudAuthorizer(BaseAuthorizer):
    def __init__(self, host, username, password, *args, **kwargs):
        super().__init__(host, username, password, *args, **kwargs)
        self.base_url = self.host + '/cloud'
        self.token = self._get_api_token()
        self.instance_id = self._get_instance_id()


    def _get_api_token(self):
        if self.token is not None:
            return self.token
        token_url = self.base_url + '/token'
        json = {'email': self.username, 'password': self.password}
        r = requests.post(token_url, json=json)
        r.raise_for_status()
        return r.content.decode('utf-8').rstrip()

    def _get_instance_id(self):
        instance_url = self.base_url +  "/instances"
        cookies = {'apiKey': self.token}
        r = requests.get(instance_url, cookies=cookies)
        r.raise_for_status()
        if 'instance' in r.cookies:
            return r.cookies['instance']
        raise Exception(f"no instance id in response cookies:{r.cookies} for requested url: {instance_url} (cookies: {cookies})")

    @property
    def auth_cookies(self):
        return {'apiKey': self.token, 'instance': self.instance_id}
