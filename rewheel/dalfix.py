import os
from itertools import ifilter

from flask import _request_ctx_stack, _app_ctx_stack as stack, request
from pydal import connection, adapters, Field
from pydal._globals import GLOBAL_LOCKER, DEFAULT
from operator import attrgetter


def field_validate(self, value, record_id = None):
    if not self.requires or self.requires == DEFAULT:
        return ((value if value != self.map_none else None), None)
    requires = self.requires
    if not isinstance(requires, (list, tuple)):
        requires = [requires]
    for validator in requires:
        (value, error) = validator(value, record_id)
        if error:
            return (value, error)
    return ((value if value != self.map_none else None), None)

Field.validate = field_validate

# fix pydal connection bug
def with_connection_or_connect(func):
    def wrap(adapter, *args, **kwargs):
        if not adapter.connection:
            adapter.reconnect()
        return func(adapter, *args, **kwargs)
    return wrap

def monkey_patch():
    meta = adapters.AdapterMeta
    decorator = adapters.with_connection_or_raise
    for adapter in ifilter(lambda x : isinstance(x,meta), attrgetter(*dir(adapters))(adapters)):
        if hasattr(adapter,'execute'):
            try:
                adapter.execute = with_connection_or_connect(adapter.execute.im_func.func_closure[0].cell_contents)
            except:
                pass

    def get_connection(self):
        return getattr(stack.top, 'dal_connection',None)

    def set_connection(self,conn):
        stack.top.dal_connection = conn

    def get_cursors(self):
        ret = getattr(stack.top, 'dal_cursors', None)
        if not ret:
            ret = {}
            stack.top.dal_cursors = ret
        return ret

    def reconnect(self):
        if getattr(stack.top, 'dal_connection',None):
            return

        if not self.pool_size:
            self.connection = self.connector()
            self.after_connection_hook()
        else:
            uri = self.uri
            POOLS = self.__class__.POOLS
            while True:
                GLOBAL_LOCKER.acquire()
                if uri not in POOLS:
                    POOLS[uri] = []
                if POOLS[uri]:
                    self.connection = POOLS[uri].pop()
                    GLOBAL_LOCKER.release()
                    try:
                        if self.check_active_connection:
                            self.test_connection()
                        break
                    except:
                        pass
                else:
                    GLOBAL_LOCKER.release()
                    self.connection = self.connector()
                    self.after_connection_hook()
                    break

    connection.ConnectionPool.connection = property(get_connection,set_connection)
    connection.ConnectionPool.cursors = property(get_cursors)
    connection.ConnectionPool.reconnect = reconnect


monkey_patch()

from pydal import DAL as oDAL

DALS = {}
APP_DALS = {}

def DAL(app, *args,**kwargs):
    db = oDAL(*args, **kwargs)
    APP_DALS[app] = (args,kwargs,db)
    return db
    # def get_db():
    #     return DALS[app]
    # return LocalProxy(get_db)

def get_db():
    app = _request_ctx_stack.top.app.blueprints[request.url_rule.endpoint.split('.')[0]]
    key = _request_ctx_stack.top
    if key in DALS:
        return DALS[key]
    else:
        # app = _request_ctx_stack.top.app.blueprints[request.url_rule.endpoint.split('.')[0]]
        args, kwargs, db = APP_DALS[app]
        ddb = oDAL(*args,**kwargs)
        for tab in db._tables:
            setattr(ddb,tab,db[tab])
            ddb[tab] = db[tab]
        return ddb

