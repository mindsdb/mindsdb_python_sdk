import time


class Predictor():
    def __init__(self, proxy, name):
        self._proxy = proxy
        self.name = name
        ok = False
        for i in range(8):
            # @TODO: Remove once https://github.com/mindsdb/mindsdb/issues/916 is fixed
            time.sleep(1)
            try:
                self.get_info()
                ok = True
                break
            except Exception as e:
                pass

        if not ok:
            raise Exception(f'Predictor {name} does not exist')

    def get_info(self):
        return self._proxy.get(f'/predictors/{self.name}')

    def delete(self):
        self._proxy.delete(f'/predictors/{self.name}')

    def predict(self, datasource, args=None):
        if args is None:
            args = {}
        if isinstance(datasource, str) or (isinstance(datasource, dict) and 'created_at' in datasource and 'updated_at' in datasource and 'name' in datasource):
            return self._proxy.post(f'/predictors/{self.name}/predict_datasource', json={
                'data_source_name':datasource
                ,'kwargs': args
            })
        else:
            return self._proxy.post(f'/predictors/{self.name}/predict', json={
                'when':datasource
                ,'kwargs': args
            })


class Predictors():
    def __init__(self, proxy):
        self._proxy = proxy

    def list_info(self):
        return self._proxy.get('/predictors')

    def  list_predictor(self):
        return [Predictor(self._proxy, x['name']) for x in self._proxy.get('/predictors')]

    def __getitem__(self, name):
        return Predictor(self._proxy, name)

    def __len__(self) -> int:
        return len(self.list_predictor())

    def __delitem__(self, name):
        self._proxy.delete(f'/predictors/{name}')

    def learn(self, name, datasource, to_predict, args=None):
        if args is None:
            args = {}
        datasource = datasource['name'] if isinstance(datasource,dict) else datasource
        self._proxy.put(f'/predictors/{name}', json={
            'data_source_name': datasource
            ,'kwargs': args
            ,'to_predict': to_predict
        })

    '''
    @TODO:
    * Add custom predictor
    * Fit custom predictor
    * Upload predictor
    * Download predictor
    * Rename predictor
    '''
