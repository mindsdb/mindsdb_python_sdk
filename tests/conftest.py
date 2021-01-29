import time
import os
from subprocess import Popen
import pytest
import psutil


@pytest.fixture(scope='session')
def mindsdb(request):
    sp = Popen(
        ['python', '-m', 'mindsdb', '--api', 'http'],
        close_fds=True
    )
    time.sleep(120)

    def fin():
        pprocess = psutil.Process(sp.pid)
        pids = [x.pid for x in pprocess.children(recursive=True)]
        pids.append(sp.pid)
        for pid in pids:
            try:
                os.kill(pid, 9)
            # process may be killed by OS due to some reasons in that moment
            except ProcessLookupError:
                pass

    request.addfinalizer(fin)
