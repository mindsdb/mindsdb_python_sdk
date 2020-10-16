import sys
if sys.version_info < (3,5):
    raise EnvironmentError('Please install a python version >= 3.5 to use this library')

from mindsdb_client.classes.mindsdb import MindsDB
