import sys
if sys.version_info < (3,5):
    raise EnvironmentError('Please install a python version >= 3.6 to use this library')

from mindsdb_sdk.classes.sdk import SDK
from mindsdb_sdk.classes.pandas_acessor import AutoML, auto_ml_config
