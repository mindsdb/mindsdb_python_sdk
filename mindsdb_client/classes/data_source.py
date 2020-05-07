from typing import Optional, Any, Tuple

class DataSource(object):
    _client: Optional['MindsDB'] = None
    _proxy: Optional['Proxy'] = None
    _data: Optional[dict] = None

    deleted: bool = False

    def __init__(self, data: dict, client: 'MindsDB'):
        self._client = client
        self._proxy = client._proxy
        self._data = data

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def _set_data(self, new_data: dict) -> None:
        _data = new_data

    def delete(self) -> bool:
        success = self._proxy.delete_datasource(self._data['name'])
        self._client.datasources.update()
        if success:
            self.deleted = True
            self._client = None
            self._proxy = None
        return self.deleted

    def analyze(self) -> dict:
        analysis = self._proxy.analyze_datasource(self._data['name'])
        return analysis

    def get_data(self) -> dict:
        data = self._proxy.get_datasource_data(self._data['name'])
        return data

    def get_file(self) -> Tuple[bytes, str]:
        content, filename = self._proxy.get_datasource_file(self._data['name'])
        return content, filename
