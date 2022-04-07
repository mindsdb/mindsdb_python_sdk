import os
import time
import requests
import json
from pathlib import Path


ENV = os.getenv('TEST_ENV', 'all')
CLOUD_HOST = "https://alpha.mindsdb.com/"

# Not used yet
def register_user(host, email, password, timeout=60):
    headers = {'Content-Type': 'application/json'}
    json = {
        "email": email,
        "password": password,
        "checked_terms": True,
        "first_name": email,
        "last_name": email,
        "invitation_code": "",
    }
    url = f"{host}/cloud/signup"
    threshold = time.time() + timeout
    to_raise = None
    while time.time() < threshold:
        try:
            res = requests.post(url, headers=headers, json=json)
            res.raise_for_status()
            to_raise = None
            break
        except Exception as e:
            to_raise = e
            time.sleep(10)
    if to_raise is not None:
        raise to_raise

# Not used yet
def doit_once(func):

    def wrapper(host):
        creds_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "credentials.txt")
        if not os.path.exists(creds_path):
            user, password = func(host)
            register_user(host, user, password)
            with open(creds_path, "w") as f:
                f.writelines([user+'\n', password+'\n'])
        else:
            with open(creds_path, "r") as f:
                user, password = f.readlines()
        return user.rstrip(), password.rstrip()
    return wrapper


# @doit_once
def generate_credentials(host):
    if host == CLOUD_HOST:
        user = os.environ.get('CLOUD_TEST_EMAIL', None)
        password = os.environ.get('CLOUD_TEST_PASSWORD', None)

        if user is None or password is None:

            creds_path = Path(__file__).parent.parent / "credentials.json"

            with open(creds_path, "r") as fd:
                creds = json.load(fd)

            user, password = creds['CLOUD_TEST_EMAIL'], creds['CLOUD_TEST_PASSWORD']

        return user, password

    raise NotImplemented
