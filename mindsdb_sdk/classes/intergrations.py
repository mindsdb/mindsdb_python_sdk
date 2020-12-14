from mindsdb_sdk.helpers.net_helpers import sending_attempts
from mindsdb_sdk.helpers.exceptions import IntegrationException


class Integration:
    def __init__(self, proxy, name):
        self._proxy = proxy
        self.name = name
        self._route = "/config/integrations"

    @sending_attempts(exception_type=IntegrationException)
    def get_info(self):
        return self._proxy.get(f'{self._route}/{self.name}')

    def update(self, params):
        """Update existing integration config.
        params -> dict: new set of integration parameters.
        {"params":{
            "enabled": true,
            "host": "localhost",
            "password": "mypass",
            "port": 3306,
            "type": "mariadb",
            "user": "root"
        }
        }
        """
        return self._proxy.post(f'{self._route}/{self.name}', json=params)

    def __len__(self):
        return len(self.get_info())


class Integrations:
    def __init__(self, proxy):
        self._proxy = proxy
        self._route = '/config/integrations'

    @sending_attempts(exception_type=IntegrationException)
    def list_info(self):
        return self._proxy.get(self._route)

    def list_integrations(self):
        return self.list_info()["integrations"]

    def __len__(self):
        return len(self.list_integrations())

    def __getitem__(self, name):
        if name in self.list_integrations():
            return Integration(self._proxy, name)
        return None

    def __setitem__(self, name, params):
        """Creates new integration config.
        params -> dict: new set of integration parameters.
        {"params":{
            "enabled": true,
            "host": "localhost",
            "password": "mypass",
            "port": 3306,
            "type": "mariadb",
            "user": "root"
        }
        }
        """
        self._proxy.put(f'{self._route}/{name}', json=params)

    def __delitem__(self, name):
        self._proxy.delete(f'{self._route}/{name}')


