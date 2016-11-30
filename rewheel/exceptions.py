import re
from import_hook import jdumps

regex_status = re.compile(r'^\d{3} [0-9A-Z ]+$')


class HTTP(Exception):
    """Raises an HTTP response

    Args:
        status: usually an integer. If it's a well known status code, the ERROR
          message will be automatically added. A string can also be passed
          as `510 Foo Bar` and in that case the status code and the error
          message will be parsed accordingly
        body: what to return as body. If left as is, will return the error code
          and the status message in the body itself
        cookies: pass cookies along (usually not needed)
        headers: pass headers as usual dict mapping
    """

    def __init__(
        self,
        status,
        body='',
        cookies=None,
        **headers
    ):
        self.status = status
        self.body = body
        self.headers = headers
        self.cookies2headers(cookies)

    def cookies2headers(self, cookies):
        if cookies and len(cookies) > 0:
            self.headers['Set-Cookie'] = [
                str(cookie)[11:] for cookie in cookies.values()]


    @property
    def message(self):
        return self.body

    def __str__(self):
        """stringify me"""
        return self.message


class Message(HTTP):
    def __init__(self, title, message=None):
        super(Message, self).__init__(512, '%s\n%s' % (title, message))


class ValidationError(Exception):
    def __init__(self, errors, resource_name):
        self.e = dict(errors=errors, _resource=resource_name)

    def __str__(self):
        return jdumps(self.e)
