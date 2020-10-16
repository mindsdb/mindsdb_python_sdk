from typing import Any, Optional

class Predictor(object):
    _client: Optional['MindsDB'] = None
    _proxy: Optional['Proxy'] = None
    _data: Optional[dict] = None

    deleted: bool = False

    def __init__(self, data: dict, client: 'MindsDB') -> None:
        self._client = client
        self._proxy = client._proxy
        self._data = data

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def _set_data(self, new_data: dict) -> None:
        _data = new_data

    def predict(self, when: dict) -> dict:
        return self._proxy.predict(self._data['name'], when)

    def download(self) -> bytes:
        return self._proxy.download_predictor(self._data['name'])

    def delete(self) -> bool:
        success = self._proxy.delete_predictor(self._data['name'])
        self._client.predictors.update()
        if success:
            self.deleted = True
            self._client = None
            self._proxy = None
        return self.deleted
