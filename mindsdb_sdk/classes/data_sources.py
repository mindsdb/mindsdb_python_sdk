from mindsdb_client.classes.data_source import DataSource
from typing import Optional, Any, List

class DataSources(object):
    _client: Optional['MindsDB'] = None
    _proxy: Optional['Proxy'] = None
    _datasources: Optional[dict] = {}

    def __init__(self, client: 'MindsDB') -> None:
        self._client = client
        self._proxy = client._proxy
        self.update()

    def __getitem__(self, key: str) -> Any:
        return self._datasources[key]

    def __len__(self) -> int:
        return len(self._datasources.keys())

    def names(self) -> List[str]:
        return list(self._datasources.keys())

    def update(self) -> None:
        data = self._proxy.get_datasources()

        new_names = [x['name'] for x in data]
        unwanted_keys = set(self._datasources.keys()) - set(new_names)
        for key in unwanted_keys:
            del self._datasources[key]

        for ds in data:
            if ds['name'] in self._datasources:
                self._datasources[ds['name']]._set_data(ds)
            else:
                self._datasources[ds['name']] = DataSource(ds, self._client)

    def add(self, name: str, path: str = None, url: str = None) -> None:
        if isinstance(path, str) and len(path) > 0:
            self._proxy.put_datasource(name, path)
        elif isinstance(url, str) and len(url) > 0:
            self._proxy.put_datasource_by_url(name, url)
        else:
            raise Exception('path or url must be declared')
        self.update()
