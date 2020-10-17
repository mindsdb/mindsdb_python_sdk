class Predictors():
    def __init__(self, proxy):
        self._proxy = proxy

    def list_info(self):
        return self._proxy.get('/predictors')

    # 
    def get(self, name):
        self._proxy.get(f'/predictors/{name}')

    def delete(self, name):
        self._proxy.delete(f'/predictors/{name}')
    
    def put(self, name, datasource, to_predict, args=None):
        if args is None:
            args = {}
        datasource = datasource['name'] if isinstance(datasource,dict) else datasource
        self._proxy.put(f'/predictors/{name}', data={
            'data_source_name': datasource
            ,'kwargs': args
            ,'to_predict': to_predict
        })
    
    def predict(self, name, datasource, args=None):
        if args is None:
            args = {}
        if isinstance(datasource, str) or (isinstance(datasource, dict) and 'created_at' in datasource and 'updated_at' in datasource and 'name' in datasource):
            return self._proxy.post(f'/predictors/{name}/predict_datasource', data={
                'data_source_name':datasource
                ,'kwargs': args
            })
        else:
            return self._proxy.post(f'/predictors/{name}/predict', data={
                'when':datasource
                ,'kwargs': args
            })

    '''
    @TODO:
    * Add custom predictor
    * Fit custom predictor
    * Upload predictor
    * Download predictor
    * Rename predictor
    '''
