from mindsdb_client.classes.predictor import Predictor
from typing import Any, List, Optional

class Predictors(object):
    _proxy: Optional['Proxy'] = None
    _client: Optional['MindsDB'] = None
    _predictors: dict = {}

    def __init__(self, client: 'MindsDB') -> None:
        self._proxy = client._proxy
        self._client = client
        self.update()
    
    def __getitem__(self, key: str) -> Any:
        return self._predictors[key]

    def __len__(self) -> int:
        return len(self._predictors.keys())

    def names(self) -> List[str]:
        return list(self._predictors.keys())

    def update(self) -> None:
        data = self._proxy.get_predictors()

        new_names = [x['name'] for x in data]
        unwanted_keys = set(self._predictors.keys()) - set(new_names)
        for key in unwanted_keys:
            del self._predictors[key]

        for p in data:
            if p['name'] in self._predictors:
                self._predictors[p['name']]._set_data(p)
            else:
                self._predictors[p['name']] = Predictor(p, self._client)

    def learn(self, name: str, data_source_name: str, to_predict: List[str]) -> 'Predictor':
        self._proxy.learn_predictor(name, data_source_name, to_predict)
        self.update()
        return self._predictors[name]

    def upload(self, file_path: str) -> bool:
        success = self._proxy.upload_predictor(file_path)
        self.update()
        return success
