try:
    import cPickle as pickle
except ImportError:
    import pickle
from datetime import timedelta
from uuid import uuid4
from redis import Redis
from werkzeug.datastructures import CallbackDict
from flask.sessions import SessionInterface, SessionMixin
from flask import request
from .import_hook import jloads
from .utils import json_mime
from .storages import NestedDict



class RedisSession(CallbackDict, SessionMixin):

    def __init__(self, initial=None, sid=None, new=False):
        def on_update(self):
            self.modified = True
        CallbackDict.__init__(self, initial, on_update)
        self.sid = sid
        self.new = new
        self.modified = False
        self.cleared = False

    def clear(self):
        """
        Clear session and mark it from removal
        :return:
        """
        super(RedisSession, self).clear()
        self.cleared = True


class RedisSessionInterface(SessionInterface):
    serializer = pickle
    session_class = RedisSession

    def __init__(self, config=None, redis=None, prefix='session:'):
        # if no redis is provided and redis is in configurarion
        # i try to create a redis instance from provided configuration
        if 'redis' in config and not redis:
            redis_args = config['redis']
            if type(redis_args) is NestedDict:
                redis_args = redis_args.main
            redis = Redis(**redis_args)
        elif redis is None:
            redis = Redis()
        self.redis = redis
        self.prefix = prefix

    def generate_sid(self):
        return str(uuid4())

    def get_redis_expiration_time(self, app, session):
        if session.permanent:
            return app.permanent_session_lifetime
        return timedelta(days=1)

    def open_session(self, app, request):
        if request.content_type == 'text/plain' and request.data:
            request._cached_json = jloads(request.data)
        sid = request.values.get('__token__')
        if not sid:
            j = request.get_json()
            if j:
                sid = j.pop('__token__', None)
        application = request.blueprint
        if not sid:
            sid = self.generate_sid()
            sess = self.session_class(sid=sid, new=True)
        else:
            val = self.redis.hget('SES %s' % application, sid)
            if val is not None:
                data = self.serializer.loads(val)
                sess =  self.session_class(data, sid=sid)
            else:
               sess = self.session_class(sid=sid, new=True)
        sess.application = application
        return sess

    def save_session(self, app, session, response):
        if session is None or session.cleared:
            self.redis.hdel('SES %s' % session.application, session.sid)
            return
        val = self.serializer.dumps(dict(session))
        self.redis.hset('SES %s' % session.application, session.sid, val)


