import sys
sys.path.insert(0,'../..')
from testingapp import application
from rewheel.sessions import RedisSessionInterface
from flask import Flask

config = dict(
    auth = dict(

    ),
    apptest = dict(
        url_prefix = '/testapp'
    )
)


app = Flask(__name__)
app.session_interface = RedisSessionInterface()
app.secret_key = 'cippa'
app.register_blueprint(application, config=config)

app.run()
