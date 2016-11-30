__author__ = 'nihil'

from time import time, sleep
from marshal import loads, dumps

zero =  dumps(0)

class RedisTouch(object):

    def __init__(self,redis, key, touched = True):
        """
        Create a redis touch on a redis server with a key
        :param redis: redis connection (Redis or StrictRedis object)
        :param key: key on redis to implement this touch
        :return:
        """
        self.red = redis
        self.key = key
        self.Key = ':TOUCH:'
        if touched:
            self.last_time = 0
        else:
            self.last_time = loads(self.red.hget(self.Key,self.key) or zero)




    def touch(self,key=None):
        """
        Touch the redis touch like linux touch command
        :return:
        """
        self.red.hset(self.Key,key or self.key,dumps(time()))

    @property
    def touched(self):
        """
        Check redis and understand touch was touched
        :return: bool
        """
        last_update = loads(self.red.hget(self.Key,self.key) or zero)
        if last_update > self.last_time:
            self.last_time = last_update
            return True
        return False

class RedisTouchCached(object):

    def __init__(self,redis,keyfunc):
        self.keyfunc = keyfunc
        self.redis = redis

    def __call__(self,func):
        cache = {}
        touches = {}
        def w(*args):
            key = self.keyfunc(args)
            if not key in touches:
                touches[key] = RedisTouch(self.redis,key)
            touch = touches[key]
            if touch.touched or key not in cache:
                cache[key] = func(*args)
            return cache[key]
        return w
