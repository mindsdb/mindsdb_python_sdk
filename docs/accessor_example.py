from mindsdb_sdk import AutoML, auto_ml_config
import pandas as pd

# The default behavior is using native, this can be done like this:
auto_ml_config(mode='native')

# We can swtich to using the API, for example on localhost, like this:
auto_ml_config(mode='api', connection_info={
    'host': 'http://localhost:47334'
})

# Or to using cloud, like this
auto_ml_config(mode='api', connection_info={
    'host': 'cloud.mindsdb.com'
    ,'user': 'george.hosu@mindsdb.com'
    ,'password': 'my_secret password'
})

# Or maybe an internal mindsdb that requires token based auth
auto_ml_config(mode='api', connection_info={
    'host': '127.34.13.22:45566'
    ,'token': 'fasgasbj#$@($@*jfbsd)!!sasd'
})

# If the `auto_ml_config` method is not called at all, then the `native` backend is used and mindsdb_native (or mindsdb) will have to be installed via pip | The flow then becomes the standard

df = pd.DataFrame({
        'x1': [x for x in range(100)]
        ,'x2': [x*2 for x in range(100)]
        ,'y': [y*3 for y in range(100)]
    })

# Train a model on the dataframe
predictor_ref = df.auto_ml.learn('y')
# Predict from the original dataframe
predictions = df.auto_ml.predict()
print(predictions[55])

test_df = pd.DataFrame({
        'x1': [x for x in range(100,110)]
        ,'x2': [x*2 for x in range(100,110)]
        ,'y': [y*3 for y in range(100,110)]
    })

# Get (run) the analysis of test_df
statistical_analysis = test_df.auto_ml.analysis
print(statistical_analysis.keys())

# Predict from the test dataframe
for pred in test_df.auto_ml.predict(predictor_ref):
    print(pred)
