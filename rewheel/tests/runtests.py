import os
import sys
import unittest
from types import ModuleType

sys.path.insert(0,'../..')
from testingapp import application
from flask import Flask
from operator import attrgetter
from threading import _start_new_thread, _sleep
from rewheel.sessions import RedisSessionInterface

config = dict(
    auth = dict(

    ),
    apptest = dict(
        url_prefix = '/apptest'
    )
)

def init_db(db):
    with app.app_context() as l:
        print l
        directory = 'testingapp/dbtestset'
        for file in os.listdir(directory):
            name, ext = os.path.splitext(file)
            if ext == 'csv':
                with open(file) as f:
                    print file
        if not db(db.auth_user).count() and not db(db.auth_group).count():
            db.auth_membership.insert(
                user_id=db.auth_user.validate_and_insert(first_name='A', last_name='B', email='a@b.com', username='a@b.com',
                                            password='pippo'),
                group_id=db.auth_group.validate_and_insert(role='admin', description='administrators'))
        db._adapter.close()

app = Flask('main')
app.register_blueprint(application,config=config)
app.session_interface = RedisSessionInterface()
app.secret_key = 'cippa'

init_db(application.db)

_start_new_thread(app.run,('localhost',1238))
_sleep(1)
import apis
if __name__ == '__main__':
    suite = unittest.TestSuite()
    for test in attrgetter(*dir(apis))(apis):
        if type(test) is ModuleType:
            print 'running test for', test.__name__
            for tester in attrgetter(*dir(test))(test):
                # if issubclass(tester,unittest.TestCase):
                #     tester.run()
                for method in dir(tester):
                    if method.startswith("test"):
                        suite.addTest(tester(method))
                test.app = application
            unittest.TextTestRunner().run(suite)
