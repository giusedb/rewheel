# DECORATORS

import re
import traceback
from logging import getLogger
from operator import *
from .storages import Storage, NestedDict

from flask import g as current
from pydal.objects import Set

from exceptions import ValidationError, HTTP, Message

log = getLogger(__name__)
ig0,ig1,ig2 = map(itemgetter,xrange(3))
json_mime = 'application/json'
re_format_fields = re.compile('%\((\S+)\)\w')

def cached(func):
    cache = {}

    def w(*a):
        if not a in cache:
            cache[a] = func(*a)
        return cache[a]

    return w


def jsoncached(func):
    cache = {}

    def w(*a):
        current.response.cached = True
        if a in cache:
            return cache[a]
        cache[a] = json(func(*a))
        return cache[a]

    return w


def call_back(cb):
    def callback_decoration(f):
        def w(*a, **b):
            cb()
            return f(*a, **b)

        return w

    return callback_decoration


def verb(func):
    def getter(id_func):
        func.id_func = id_func
        return func

    func.verb = True
    func.func_id = getter
    return func


def jsoned(func):
    def x():
        try:
            ret = func()
            current.response.headers['Content-Type'] = 'application/json'
            if current.response.cached:
                return ret
            if hasattr(ret, 'as_json'):
                return ret.as_json()
            return json(ret)
        except Message as e:
            raise e
        except ValidationError as e:
            e.e['formIdx'] = current.response.formIdx
            raise HTTP(513, str(e))
        except Unauthorized as e:
            raise HTTP(401, e.body)
        except HTTP as e:
            raise e
        except Exception as e:
            current.db.rollback()
            current.response.do_not_commit = True
            if current.globalenv.get('debug'):
                raise HTTP(511,
                           json(dict(exception=str(e), traceback='\n'.join(traceback.format_tb(sys.exc_info()[2])))))
            else:
                raise e

    x.__name__ = func.__name__
    return x


def no_got_model(func):
    @verb
    def w(*a, **b):
        current.response.headers['nomodel'] = True
        return func(*a, **b)

    w.func_name = func.func_name
    return w


def field_from_format(tab):
    """
    find fields from format of a table and returns the list of fields who build it
    :param tab: gluon.dal.Table object
    :return: list o fields composing table format string
    """
    return itemgetter(*re_format_fields.findall(tab._format))(tab) + (tab.id,)

def column(db, q, col, **kwargs):
    if type(q) != Set:
        q = db(q)
    return map(itemgetter(0), db.executesql(q._select(col, **kwargs)))

def sql(db, q, *fields, **kwargs):
    if type(q) != Set:
        q = db(q)
    as_dict = kwargs.pop('as_dict',True)
    sql = q._select(*fields, **kwargs)
    # log.debug(sql)
    return db.executesql(sql, as_dict=as_dict)

def sql_decoded(q, *fields,**kwargs):
    if type(q) != Set:
        q = current.db(q)
    as_dict = kwargs.pop('as_dict',True)
    if as_dict:
        decoders = dict((field.name,field.type.decoder) for field in fields if type(field) is Field and hasattr(field.type,'decoder'))
    else:
        decoders = dict((n,field.type.decoder) for n,field in enumerate(fields) if type(field) is Field and hasattr(field.type,'decoder'))
    ret = current.db.executesql(q._select(*fields, **kwargs), as_dict=as_dict)
    if as_dict:
        for idx, func in decoders.iteritems():
            for row in ret:
                row[idx] = func(row[idx])
        return ret
    else:
        decs = set(decoders)
        return tuple(tuple(decoders[idx](row[idx]) if idx in decs else row[idx] for idx, value in enumerate(row)) for row in ret)

