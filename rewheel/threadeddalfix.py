def monkey_patch():
    from pydal import adapters

    def with_connection_or_connect(func):
        def wrap(adapter, *args, **kwargs):
            if not adapter.connection:
                adapter.reconnect()
            return func(adapter, *args, **kwargs)
        return wrap

    apts = adapters.adapters._registry_
    for adapter_name, adapter in apts.items():
        if hasattr(adapter, 'execute'):
            try:
                adapter.execute = with_connection_or_connect(adapter.execute.im_func.func_closure[0].cell_contents)
            except:
                pass

def monkey_patch2():
    from pydal._globals import THREAD_LOCAL, GLOBAL_LOCKER
    from pydal.connection import ConnectionPool as CP

    def get_connection(self):
        ret = getattr(THREAD_LOCAL, self._connection_thname_, None)
        if not ret:
            self.reconnect()
            return self.connection
        return ret

    def set_connection(self, val):
        setattr(THREAD_LOCAL, self._connection_thname_, val)
        self._clean_cursors()
        if val is not None:
            self._build_cursor()

    def reconnect(self):
        """
        Defines: `self.connection` and `self.cursor`
        if `self.pool_size>0` it will try pull the connection from the pool
        if the connection is not active (closed by db server) it will loop
        if not `self.pool_size` or no active connections in pool makes a new one
        """
        if getattr(THREAD_LOCAL, self._connection_thname_, None) is not None:
            return
        # print 'reconnecting'
        if not self.pool_size:
            self.connection = self.connector()
            self.after_connection_hook()
        else:
            uri = self.uri
            POOLS = CP.POOLS
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


    CP.connection = property(get_connection, set_connection)
    CP.reconnect = reconnect


monkey_patch()
monkey_patch2()

from pydal import DAL as oDAL, Field

def DAL(app,*args, **kwargs):
    db = oDAL(*args,**kwargs)
    db._app = app
    app.db = db
    return db

