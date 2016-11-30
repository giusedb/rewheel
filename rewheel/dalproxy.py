#!/usr/bin/env python
# coding: utf8

from pydal import DAL, Field
from pydal._globals import THREAD_LOCAL
from threading import current_thread as ct, Lock as LazyLock
from operator import attrgetter
from flask import request


statics = {}
DALS = {}
DAL_PROXY = None
DAL_ARGS = None
oDAL = DAL
first_db = None

first_db_lock = LazyLock()

def future_commit():
    if not hasattr(THREAD_LOCAL, 'db_instances'):
        THREAD_LOCAL.db_instances = {}
    db_uid = current.db._db_uid
    db_group = THREAD_LOCAL.db_instances.get(db_uid, [])
    if current.db not in db_group:
        db_group.append(current.db)
        THREAD_LOCAL.db_instances[db_uid] = db_group
    # else:
    #     print 'Gia fatto'

def find_db():
    thread = ct()
    try:
        ret = DALS[thread]
        # if first_db == current.db
        # future_commit()
        ret._adapter.reconnect()
    except KeyError:
        fdb = first_db
        ret = DALS[thread] = oDAL(DAL_ARGS[0],*DAL_ARGS[1],**DAL_ARGS[2])
        if fdb.tables:
            for tab in attrgetter(*fdb.tables)(fdb):
                setattr(ret,tab._tablename,tab)
        else:
            print 'EEEEE : vecchio DB senza tabelle'
        ret._tables = fdb._tables
    return ret



def set_db(db):
    DALS[ct()] = db

def DAL(cs,*args,**kwargs):
    global DAL_PROXY
    global DAL_ARGS
    global first_db
    # current.update_log = {}
    if DAL_PROXY:
        return DAL_PROXY
    # first_db_lock.acquire()
    DAL_ARGS = cs,args,kwargs
    first_db = oDAL(cs,*args,**kwargs)
    DAL_PROXY = DALProxy(first_db)
    DAL_PROXY.proxy = DAL_PROXY
    return DAL_PROXY

class DALProxy(object):
    __slots__ = ["_obj", "__weakref__"]
    def __init__(self, obj):
        set_db(obj)
    #
    # proxying (special cases)
    #
    def __getattribute__(self, name):
        return getattr(find_db(), name)
    def __delattr__(self, name):
        delattr(find_db(), name)
    def __setattr__(self, name, value):
        setattr(find_db(), name, value)

    def __nonzero__(self):
        return True
    def __str__(self):
        return str(find_db())
    def __repr__(self):
        return repr(find_db())

    #
    # factories
    #
    _special_names = [
        '__abs__', '__add__', '__and__', '__call__', '__cmp__', '__coerce__',
        '__contains__', '__delitem__', '__delslice__', '__div__', '__divmod__',
        '__eq__', '__float__', '__floordiv__', '__ge__', '__getitem__',
        '__getslice__', '__gt__', '__hash__', '__hex__', '__iadd__', '__iand__',
        '__idiv__', '__idivmod__', '__ifloordiv__', '__ilshift__', '__imod__',
        '__imul__', '__int__', '__invert__', '__ior__', '__ipow__', '__irshift__',
        '__isub__', '__iter__', '__itruediv__', '__ixor__', '__le__', '__len__',
        '__long__', '__lshift__', '__lt__', '__mod__', '__mul__', '__ne__',
        '__neg__', '__oct__', '__or__', '__pos__', '__pow__', '__radd__',
        '__rand__', '__rdiv__', '__rdivmod__', '__reduce__', '__reduce_ex__',
        '__repr__', '__reversed__', '__rfloorfiv__', '__rlshift__', '__rmod__',
        '__rmul__', '__ror__', '__rpow__', '__rrshift__', '__rshift__', '__rsub__',
        '__rtruediv__', '__rxor__', '__setitem__', '__setslice__', '__sub__',
        '__truediv__', '__xor__', 'next',
    ]

    @classmethod
    def _create_class_proxy(cls, theclass):
        """creates a proxy for the given class"""

        def make_method(name):
            def method(self, *args, **kw):
                return getattr(find_db(), name)(*args, **kw)
            return method

        namespace = {}
        for name in cls._special_names:
            if hasattr(theclass, name):
                namespace[name] = make_method(name)
        return type("%s(%s)" % (cls.__name__, theclass.__name__), (cls,), namespace)

    def __new__(cls, obj, *args, **kwargs):
        """
        creates an proxy instance referencing `obj`. (obj, *args, **kwargs) are
        passed to this class' __init__, so deriving classes can define an
        __init__ method of their own.
        note: _class_proxy_cache is unique per deriving class (each deriving
        class must hold its own cache)
        """
        try:
            cache = cls.__dict__["_class_proxy_cache"]
        except KeyError:
            cls._class_proxy_cache = cache = {}
        try:
            theclass = cache[obj.__class__]
        except KeyError:
            cache[obj.__class__] = theclass = cls._create_class_proxy(obj.__class__)
        ins = object.__new__(theclass)
        theclass.__init__(ins, obj, *args, **kwargs)
        return ins

