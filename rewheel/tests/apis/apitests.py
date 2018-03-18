import unittest
import requests
import json

baseurl = 'http://localhost:1238/apptest/'

class APITest(unittest.TestCase):
    def setUp(self):
        self.baseurl = baseurl


    def to_json(self, response):
        try:
            return json.loads(response.text)
        except:
            self.fail(('%s is not jsonable from %s URL' % (response.text, response.url)))


    def get(self, url):
        ret = requests.get(self.baseurl + url)
        self.check_header(ret)
        return ret


    def post(self, url, data=None, json=None):
        ret = requests.post(self.baseurl + url, data=data, json=json,allow_redirects = False)
        self.check_header(ret)
        return self.to_json(ret), ret.status_code


    def check_header(self, res):
        self.assertEqual(res.headers['Content-type'], 'application/json',
                         'Header is %s. it have to be application/json for url %s' % (res.headers['Content-type'], res.url))

