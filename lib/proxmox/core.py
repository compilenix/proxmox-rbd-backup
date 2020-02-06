__author__ = 'Oleg Butovich'
__copyright__ = '(c) Oleg Butovich 2013-2017'
__licence__ = 'MIT'

import posixpath
from requests.cookies import cookiejar_from_dict
from .https import Backend, ProxmoxHTTPAuth
from ..helper import Log as log
from http import client as httplib
from urllib import parse as urlparse
basestring = (bytes, str)


# https://metacpan.org/pod/AnyEvent::HTTP
ANYEVENT_HTTP_STATUS_CODES = {
    595: "Errors during connection establishment, proxy handshake",
    596: "Errors during TLS negotiation, request sending and header processing",
    597: "Errors during body receiving or processing",
    598: "User aborted request via on_header or on_body",
    599: "Other, usually nonretryable, errors (garbled URL etc.)"
}


class ProxmoxResourceBase(object):

    def __getattr__(self, item):
        if item.startswith("_"):
            raise AttributeError(item)

        kwargs = self._store.copy()
        kwargs['base_url'] = self.url_join(self._store["base_url"], item)

        return ProxmoxResource(**kwargs)

    def url_join(self, base, *args):
        scheme, netloc, path, query, fragment = urlparse.urlsplit(base)
        path = path if len(path) else "/"
        path = posixpath.join(path, *[('%s' % x) for x in args])
        return urlparse.urlunsplit([scheme, netloc, path, query, fragment])


class ResourceException(Exception):
    pass


class ProxmoxResource(ProxmoxResourceBase):

    def __init__(self, **kwargs):
        self._store = kwargs

    def __call__(self, resource_id=None):
        if not resource_id:
            return self

        if isinstance(resource_id, basestring):
            resource_id = resource_id.split("/")
        elif not isinstance(resource_id, (tuple, list)):
            resource_id = [str(resource_id)]

        kwargs = self._store.copy()
        if resource_id is not None:
            kwargs["base_url"] = self.url_join(self._store["base_url"], *resource_id)

        return self.__class__(**kwargs)

    def _request(self, method, data=None, params=None):
        url = self._store["base_url"]
        if data:
            log.debug(f'{method} {url} {data}')
        else:
            log.debug(f'{method} {url}')
        resp = self._store["session"].request(method, url, data=data or None, params=params)
        log.debug(f'Status code: {resp.status_code}, output: {resp.content}')

        if resp.status_code == 401:
            log.debug(f'Received 401, the current session may have expired. Retry renewing it.')
            tmp_url = urlparse.urlparse(self._store["base_url"])
            tmp_url = f'{tmp_url.scheme}://{tmp_url.netloc}/api2/json'
            self._store['session'].auth = ProxmoxHTTPAuth(tmp_url,
                                                          self._store['session'].auth.username,
                                                          self._store['session'].auth.password,
                                                          self._store['session'].auth.verify_ssl)
            self._store['session'].cookies = cookiejar_from_dict({"PVEAuthCookie": self._store['session'].auth.pve_auth_cookie})
            log.debug('Retry original request.')
            return self._request(method, data=data, params=params)

        if resp.status_code >= 400:
            if hasattr(resp, 'reason'):
                raise ResourceException("{0} {1}: {2} - {3}".format(
                    resp.status_code,
                    httplib.responses.get(resp.status_code,
                                          ANYEVENT_HTTP_STATUS_CODES.get(resp.status_code)),
                    resp.reason, resp.content))
            else:
                raise ResourceException("{0} {1}: {2}".format(
                    resp.status_code,
                    httplib.responses.get(resp.status_code,
                                          ANYEVENT_HTTP_STATUS_CODES.get(resp.status_code)),
                    resp.content))
        elif 200 <= resp.status_code <= 299:
            return self._store["serializer"].loads(resp)

    def get(self, *args, **params):
        return self(args)._request("GET", params=params)

    def post(self, *args, **data):
        return self(args)._request("POST", data=data)

    def put(self, *args, **data):
        return self(args)._request("PUT", data=data)

    def delete(self, *args, **params):
        return self(args)._request("DELETE", params=params)

    def create(self, *args, **data):
        return self.post(*args, **data)

    def set(self, *args, **data):
        return self.put(*args, **data)


class ProxmoxAPI(ProxmoxResourceBase):
    def __init__(self, host, backend='https', **kwargs):

        # load backend module
        self._backend = Backend(host, **kwargs)
        self._backend_name = backend

        self._store = {
            "base_url": self._backend.get_base_url(),
            "session": self._backend.get_session(),
            "serializer": self._backend.get_serializer(),
        }

    def get_tokens(self):
        """Return the auth and csrf tokens.

        Returns (None, None) if the backend is not https.
        """
        if self._backend_name != 'https':
            return None, None

        return self._backend.get_tokens()
