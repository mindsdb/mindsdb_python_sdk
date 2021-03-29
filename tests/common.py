import os
import time
import requests


CLOUD_HOST = "https://stockholm_0_http.mindsdb.com/"

def register_user(host, email, password, timeout=2400):
    invitation_code = os.getenv("CLOUD_INVITE_CODE", None)
    if invitation_code is None:
        raise Exception("Unable to find invitation code in existed environment variables.")
    headers = {'Content-Type': 'application/json'}
    json = {"email": email,
            "password": password,
            "invitation_code": invitation_code}
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


@doit_once
def generate_credentials(host):
    now = time.time()
    user = f"{now}@sdktest.com"
    password = f"{int(now)}"
    return user, password
