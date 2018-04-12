from __future__ import print_function, division, absolute_import

import hmac
import json
import os
import select
import socket
import struct
import subprocess
from base64 import b64encode
from collections import MutableMapping
from contextlib import closing
from hashlib import sha1, md5

import requests

from .exceptions import (UnauthorizedError, ResourceManagerError,
                         ApplicationMasterError)
from .utils import ensure_bytes


_SECRET_ENV_VAR = b'CROCHET_SECRET_ACCESS_KEY'


class SimpleAuth(requests.auth.AuthBase):
    """Implement simple authentication for the yarn REST api"""
    def __init__(self, user):
        self.user = user

    def __call__(self, r):
        sym = '&' if '?' in r.url else '?'
        r.url += '{sym}user.name={user}'.format(sym=sym, user=self.user)
        return r


class CrochetAuth(requests.auth.AuthBase):
    def __init__(self, secret):
        self.secret = secret

    def __call__(self, r):
        method = ensure_bytes(r.method)
        content_type = ensure_bytes(r.headers.get('content-type', b''))
        path = ensure_bytes(r.path_url)
        if r.body is not None:
            body = ensure_bytes(r.body)
            body_md5 = md5(body).digest()
        else:
            body_md5 = b''

        msg = b'\n'.join([method, body_md5, content_type, path])
        mac = hmac.new(self.secret, msg=msg, digestmod=sha1)
        signature = b64encode(mac.digest())

        r.headers['Authorization'] = b'CROCHET %s' % signature
        return r


class Client(object):
    def __init__(self, secret=None):
        if secret is None:
            secret = os.environb.get(_SECRET_ENV_VAR)
            if secret is None:
                raise ValueError("Secret not provided, and not found at "
                                 "%r" % _SECRET_ENV_VAR.decode())

        self._secret = secret
        self._auth = CrochetAuth(secret)
        self._init_client()

    def _init_client(self):
        command = ["yarn", "jar", "crochet-1.0-SNAPSHOT-jar-with-dependencies.jar"]

        callback = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        callback.bind(('127.0.0.1', 0))
        callback.listen(1)

        with closing(callback):
            _, callback_port = callback.getsockname()

            env = dict(os.environ)
            env.update({'CROCHET_SECRET_ACCESS_KEY': self._secret,
                        'CROCHET_CALLBACK_PORT': str(callback_port)})

            proc = subprocess.Popen(command, stdin=subprocess.PIPE, env=env,
                                    start_new_session=True)

            while proc.poll() is None:
                readable, _, _ = select.select([callback], [], [], 1)
                if callback in readable:
                    connection = callback.accept()[0]
                    with closing(connection):
                        stream = connection.makefile(mode="rb")
                        msg = stream.read(4)
                        if not msg:
                            raise ValueError("Failed to read in client port")
                        port = struct.unpack("!i", msg)[0]
                        break

        self._address = 'http://127.0.0.1:%d' % port
        self._proc = proc

    def _handle_exceptions(self, resp):
        if resp.status_code == 401:
            raise UnauthorizedError("Failed to authenticate with Server")
        else:
            raise ResourceManagerError("Server responded with an unhandled "
                                       "status code: %d" % resp.status_code)

    def submit(self):
        url = '%s/apps/' % self._address
        resp = requests.post(url, auth=self._auth)
        if resp.status_code != 200:
            self._handle_exceptions(resp)
        app_id = resp.content.decode()

        return Application(self, app_id)

    def status(self, appid):
        url = '%s/apps/%s' % (self._address, appid)
        resp = requests.get(url, auth=self._auth)
        if resp.status_code == 200:
            return resp.json()
        self._handle_exceptions(resp)


class KeyStore(MutableMapping):
    """Wrapper for the Crochet KeyStore"""
    def __init__(self, address, auth):
        self._address = address
        self._auth = auth

    def __repr__(self):
        return 'KeyStore<address=%s>' % self._address

    def _handle_exceptions(self, resp):
        if resp.status_code == 401:
            raise UnauthorizedError("Failed to authenticate with "
                                    "ApplicationMaster")
        else:
            raise ApplicationMasterError("ApplicationMaster responded with an "
                                         "unhandled status code: "
                                         "%d" % resp.status_code)

    def _check_key(self, key):
        if not len(key):
            raise ValueError("len(key) must be > 0")

    def _list_keys(self):
        url = '%s/keys/' % self._address
        resp = requests.get(url, auth=self._auth)
        if resp.status_code != 200:
            self._handle_exceptions(resp)
        return json.loads(resp.content)['keys']

    def __getitem__(self, key):
        self._check_key(key)
        url = '%s/keys/%s' % (self._address, key)
        resp = requests.get(url, auth=self._auth)
        if resp.status_code == 200:
            return resp.content.decode()
        elif resp.status_code == 404:
            raise KeyError(key)
        else:
            self._handle_exceptions(resp)

    def __setitem__(self, key, value):
        self._check_key(key)
        url = '%s/keys/%s' % (self._address, key)
        resp = requests.put(url, auth=self._auth, data=value)
        if resp.status_code != 204:
            self._handle_exceptions(resp)

    def __delitem__(self, key):
        self._check_key(key)
        url = '%s/keys/%s' % (self._address, key)
        resp = requests.delete(url, auth=self._auth)
        if resp.status_code == 404:
            raise KeyError(key)
        elif resp.status_code != 204:
            self._handle_exceptions(resp)

    def __iter__(self):
        return iter(self._list_keys())

    def __len__(self):
        return len(self._list_keys())


class Application(object):
    def __init__(self, client, app_id):
        self.client = client
        self.app_id = app_id

    def __repr__(self):
        return 'Application<id=%r>' % self.app_id