from apitests import APITest
import requests
import os
import json
from utils import randword
from rewheel.utils import sql

class AuthTester(APITest):

    def test_get_login(self):
        res = self.get('api/login')
        self.assertEqual(res.status_code,401,'login doesn\'t returns 404')


    def test_login_whithout_args(self):
        res, status = self.post('api/login')
        self.assertNotEqual(status,401,'Login api returns %s rather then 401' % status)


    def test_user_no_password(self):
        db = app.db
        emails, usernames = zip(*sql(db, db.auth_user, 'email', 'username', as_dict=False))
        for x in (randword(6) for _ in range(10)):
            if x not in emails and x not in usernames:
                res, status = self.post('api/login',data=dict(username = x))
                self.assertEqual(status,400,'Incorrect user is not 400')
                self.assertIn('error', res, 'Incorrect login doesn\'t returns error')


    def test_user_pass(self):
        db = app.db
        emails, usernames = zip(*db().select(db.auth_user.email, db.auth_user.username))
        for x in (randword(6) for _ in range(10)):
            if x not in emails and x not in usernames:
                res, status = self.post('api/login', data=dict(username=x, password=randword(40)))
                self.assertEqual(status, 401, 'Incorrect user is not 401')
                self.assertIn('error', res, 'Incorrect login doesn\'t returns error')

    def test_correct_user(self):
        db = app.db
        res, status = self.post('api/login',data=dict(username = 'a@b.com', password='pippo'))
        keys = 'token','user_id','apiEndPoint','templates','realtimeEndPoint'
        if not set(keys).issubset(res):
            self.fail('correct login returns %s keys, but %s has to be returned' % (res.keys(), keys))
        self.assertEqual(status, 200, 'Correct user have to return 200')

    def test_logout(self):
        # login
        res, status = self.post('api/login',data=dict(username = 'a@b.com', password='pippo'))
        token = res['token']
        res, status = self.post('api/logout', data=dict(_token_=token))
        print res, status
        res, status = self.post('api/logout',data={})
        if not 'error' in res:
            self.fail('No error returned on logout')

if __name__ == '__main__':
    test = AuthTester()
    test.run()