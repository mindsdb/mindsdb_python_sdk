from mindsdb_client.classes.predictor import Predictor

class Predictors(object):
    _proxy = None
    _predictors = {}
    def __init__(self, proxy):
        self._proxy = proxy
        self.update()
    
    def __getitem__(self, key):
        return self._predictors[key]

    def names(self):
        return list(self._predictors.keys())

    def update(self):
        data = self._proxy.get_predictors()

        new_names = [x['name'] for x in data]
        unwanted_keys = set(self._predictors.keys()) - set(new_names)
        for key in unwanted_keys:
            del self._predictors[key]

        for p in data:
            if p['name'] in self._predictors:
                self._predictors[p['name']].update(p)
            else:
                self._predictors[p['name']] = Predictor(p, self._proxy)

    def learn(self, name, data_source_name, to_predict):
        self._proxy.learn_predictor(name, data_source_name, to_predict)
        self.update()

    def delete(self, name):
        self._proxy.delete_predictor(name)
        self.update()
