import sys
if sys.version_info < (3,6):
    sys.exit('Sorry, For MindsDB Client does not support python < 3.6')

from mindsdb_client.classes.mindsdb import MindsDB
