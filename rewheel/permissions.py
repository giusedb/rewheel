__author__ = 'nihil'

# pemission definers

from marshal import dumps

from types import MethodType
from .main import *
from .exceptions import HTTP
from logging import getLogger

logger = getLogger('rewheel.permissions')


class Unauthorized(HTTP):
    def __init__(self,message):
        super(Unauthorized,self).__init__(401,message)

class Permission(object):

    query = None
    joins = tuple()
    permission = None
    last_table = None
    p0 = set()

    def get_groups(self,items):
        """
        find user groups to send items
        :return:
        """
        raise NotImplemented('get_groups() not implemented for %s' % self.__class__.__name__)

    def rt_serialize(self):
        raise NotImplemented('Please subclass %s and refine function rt_serialize' % type(self).__name__)

    def changed(self,a,b):
        return False

    def get_objects(self,id):
        return ()

    def __or__(self, other):
        return OrPermission(self,other)

    def __and__(self, other):
        return AndPermission(self,other)

class MembershipPermission(Permission):

    def __init__(self, app, *group_names):
        """
        Grants permission on a method if user is member of named group
        :param group_name: group name
        :param func: function to call if check is successful
        """
        self.app = app
        self.auth = app.auth
        self.group_ids = tuple(imap(app.auth.id_group,group_names))
        if not all(self.group_ids):
            unlinked = imap(itemgetter(0), ifilter(lambda x : not x[1], izip(group_names, self.group_ids)))
            logger.warning('unable to get group id for followings : %s' % ','.join(unlinked))
            self.group_ids = filter(bool,self.group_ids)

    def __call__(self, func):
        def check_membership(resource,*args,**kwargs):
            cp = kwargs.pop('_check_permissions',True)
            if cp:
                logger.debug('check membership permission for %s' % resource.name)
                auth = self.app.auth
                if set(self.group_ids).intersection(auth.user_groups):
                    return func(*args,**kwargs)
                try:
                    log.info('User %s is not authorized to call %s.%s' % (self.auth.user_id, func.im_self.name, func.__name__))
                except:
                    pass
                raise Unauthorized("Permesso negato all'utente %s %s." % (auth.user.first_name,auth.user.last_name))
            return func(*args,**kwargs)
        return check_membership

    @property
    def query(self):
        auth = self.auth
        if set(self.group_ids).intersection(auth.user_groups):
            return None
        raise Unauthorized('Permesso negato per %(first_name)s %(last_name)s' % auth.user)

    def rt_serialize(self):
        return dumps(['membership',self.group_ids])

    def get_groups(self,idx_items):
        """
        Get group from given items
        :param idx_items: dict as { id : item }
        :return: dict as { id : { group set }}
        """
        return dict((id,self.group_ids) for id in imap(itemgetter('id'),idx_items))

class DirectPermission(Permission):

    def __init__(self,app, permission_name, id_func = None):
        self.permission = permission_name
        self.id_func = id_func
        self.traversing = []
        self.auth = app.auth
        self.db = app.db

    def __call__(self, func):
        res = func.im_self
        self.first_name = self.traversing[0] if self.traversing else 'id'
        pt = current.db.auth_permission
        self.resource = res
        self.table = res.table
        self.last_table = self.last_table or res.table
        self.p0 = set(column(self.db, (pt.record_id == 0) & (pt.name == self.permission) & (pt.table_name == self.last_table),pt.group_id))
        logger.debug('PM : setting permission based permission on %s for %s verb.' % (self.table._tablename,func.func_name))
        if self.id_func or hasattr(func.im_func,'id_func'):
            if not self.id_func:
                self.id_func = getattr(func,'id_func')
            def check_direct(resource,*args,**kwargs):
                # is an update
                if kwargs.pop('_check_permissions',True):
                    logger.debug('check direct permission for %s' % resource.name)
                    id = self.id_func(args,kwargs)
                    db = current.db
                    p = db.auth_permission
                    auth = self.auth
                    if id == None and self.traversing:
                        ref = kwargs.get(self.traversing[0].name)
                        id = column(self.db, self.traversing[0].referent == ref,self.last_table._id,left=self.joins[1:])
                    if id :
                        if db((p.name == self.permission) & (p.table_name == self.last_table) & p.group_id.belongs(auth.user_groups) & (p.record_id.belongs([id,0] if isNumberType(id) else list(id) + [0]))).count():
                            return func(*args,**kwargs)
                        else:
                            raise Unauthorized('Permesso negato')
                    else:
                        raise Unauthorized("Permesso negato su un non oggetto")
                else:
                    return func(*args,**kwargs)
            return check_direct
        return func

    @property
    def query(self):
        return self.auth.accessible_query(self.permission,self.table)

    def rt_serialize(self):
        return dumps(['direct',self.permission])

    def get_groups(self,items):
        """
        Get group from given items
        :param idx_items: dict as { id : item }
        :return: dict as { id : { group set }}
        """
        idx_items = dict((item['id'],item) for item in items)
        pt = current.db.auth_permission
        res = self.sql((pt.table_name == self.table._tablename) & (pt.name == self.permission) & pt.record_id.belongs(idx_items),pt.record_id,pt.group_id,as_dict=False,orderby=pt.record_id)
        ret = dict((k,tuple(sorted(self.p0.union(imap(ig1,g))))) for k,g in groupby(res,ig0))
        ret.update((id,tuple(sorted(self.p0))) for id in set(idx_items).difference(ret))
        return ret

    def get_objects(self,id):
        """
        Get all object related to ginven id object by its permissions
        :param id:
        :return:
        """
        return self.sql(self.table._id == id,*self.resource.visible_fields)

class DelegatePermission(DirectPermission):

    def __init__(self, app, permission_name, traversing = [], id_func = None):
        """
        Delegate a permission to another object with a permission name
        :param permission_name: permission name to check
        :param traversing: crossing path from real object to defined permission (similar to left join)
                    ie [['comment','document'],['doc','parent']]
        :return:
        """
        traversing = list(traversing)
        self.id_func = id_func
        self.permission = permission_name
        self.traversing = traversing
        self.field = traversing[0]
        self.auth = app.auth
        self.db = self.auth.db
        self.sql = app.sql

    def __call__(self, func):
        ret = super(DelegatePermission,self).__call__(func)
        db = current.db
        pt = db.auth_permission
        # making field traversing
        traversing = list(self.traversing)
        self.traversing = []
        curr_table = self.table
        while traversing:
            field = db[curr_table][traversing.pop(0)]
            self.traversing.append(field)
            try:
                curr_table = field.referent.table
            except AttributeError:
                if field.type.startswith('reference'):
                    curr_table = db[field.type.split(' ')[-1]]
                    field.referent = curr_table

        self.last_table = curr_table

        self.joins = tuple((field.referent.table.on(field == field.referent.table._id)) for field in self.traversing)

        if len(self.joins) > 1:
            self.permission_join = self.joins[1:-1]
            self.permission_field = self.traversing[0]
        else:
            self.permission_join = None # (pt.on((pt.table_name == self.last_table) & (pt.record_id == self.table[self.field])),)
            self.permission_field = pt.record_id
        self.p0 = set(column(self.db, (pt.table_name == self.last_table._tablename) & (pt.record_id == 0) & (pt.name == self.permission),pt.group_id))

        return ret

    @property
    def query(self):
        return self.auth.accessible_query(self.permission,self.last_table)

    def get_groups(self,items):
        """
        Get group from given items
        :param idx_items: dict as { id : item }
        :return: dict as { id : { group set }}
        """
        fid = itemgetter(self.field)
        pt = current.db.auth_permission

        # indexing id by permission based key field
        idx_field_id = dict((field,set(imap(itemgetter('id'),group))) for field,group in groupby(sorted(items,key=fid),fid))
        query = self.permission_field.belongs(idx_field_id) & (pt.name == self.permission) & (pt.table_name == self.last_table)
        res = dict((k,tuple(sorted(self.p0.union(imap(ig1,g))))) for k,g in groupby(self.sql(query,self.permission_field,pt.group_id,left=self.permission_join,as_dict=False),ig0))
        ret = dict((id,v) for k,v in res.iteritems() for id in idx_field_id[k])
        ret.update((id,tuple(sorted(self.p0))) for id in reduce(set.union,idx_field_id.itervalues(),set()).difference(ret))
        return ret

    def changed(self,a,b):
        """
        check difference from a to b and determine if this changes influences permissions
        :param a: dict item
        :param b: dict item
        :return: bool
        """
        return a[self.field] != b[self.field]

    def get_objects(self,id):
        return self.sql(self.last_table._id == id,*self.resource.visible_fields,left=self.joins)

class OwnerPermission(Permission):

    user_groups = dict()

    def __init__(self,app, traversing, id_func=None):
        """
        Create a permission validator to grant permission only if connected user is linked by a field on a defined object
        :param traversing: list of pairs [table,field]
        """
        self.traversing = traversing.split('.')
        self.id_func = id_func
        self.auth = app.auth

    def __call__(self, func):
        db = current.db
        res = func.im_self
        self.resource = res
        self.table = res.table
        traversing = list(self.traversing)[:-1]
        last_field_name = self.traversing[-1]
        self.traversing = []
        curr_table = self.table
        while traversing:
            field = db[curr_table][traversing.pop(0)]
            self.traversing.append(field)
            try:
                curr_table = field.referent.table
            except AttributeError:
                if field.type.startswith('reference'):
                    curr_table = db[field.type.split(' ')[-1]]
                    field.referent = curr_table

        self.joins = tuple((field.referent.table.on(field == field.referent.table._id)) for field in self.traversing)

        self.last_table = curr_table
        self.user_field = curr_table[last_field_name]

        logger.debug('PM : setting attribute based permissions on %s for verb %s.' % (self.table._tablename,func.func_name))

        if self.id_func or hasattr(func.im_func,'id_func'):
            if not self.id_func:
                self.id_func = getattr(func,'id_func')
            def check_ownership(resource,*args,**kwargs):
                # is an update
                if kwargs.pop('_check_permissions',True):
                    logger.debug('check ownership permission for %s' % resource.name)
                    user_id = self.user_id
                    id = self.id_func(args,kwargs)
                    if id:
                        if type(id) in (tuple,list,set):
                            query = resource.table.id.belongs(id)
                        else:
                            query = resource.table.id == id
                        if user_id.intersection(column(self.db, query,self.user_field,left=self.joins)):
                            return func(*args,**kwargs)
                    else:
                        if self.traversing:
                            if len(traversing) > 1:
                                if column(self.db, (self.user_field.belongs(user_id)) & (self.traversing[1].table._id == kwargs.get(self.traversing[0].name)),self.last_table._id,left=self.joins[1:]):
                                    return func(*args,**kwargs)
                            else:
                                if column(self.db, (self.user_field.belongs(user_id)) & (self.last_table.id == kwargs.get(self.traversing[0].name)),self.last_table._id,left=self.joins[1:]):
                                    return func(*args, **kwargs)
                        else:
                            if kwargs.get(self.user_field.name) in user_id:
                                return func(*args,**kwargs)
                    raise Unauthorized("Permesso negato")
                else:
                    return func(*args,**kwargs)
            return check_ownership
        return func

    def resolve_user(self,user_id):
        if user_id not in self.user_groups:
            r = column(self.db, self.db.auth_group.role == 'user_%s' % user_id,'id')
            self.user_groups[user_id] = (r and r[0]) or None
        return self.user_groups[user_id]

    @property
    def user_id(self):
        return set((self.auth.user_id,))

    @property
    def query(self):
        return self.user_field.belongs(self.user_id)

    def get_groups(self,items):
        ids = map(itemgetter('id'),items)
        return dict((id,(self.resolve_user(user),)) for id, user in self.sql(self.table._id.belongs(ids),self.table._id,self.user_field,left=self.joins,as_dict=False))

    def get_objects(self,id):
        # TODO creare la get objects per l'Ownership Permission
        return []

class GroupOnPermission(OwnerPermission):
    """
    Defines permissions for user is member of a group defined as an attribute on DB entity
    I.E.: You have table like this :
        A(id,name,group)
        B(id,name,a_id)
        C(id,name.b_id)
    and you want to c is accessible to users with membership on group linked by A you can set
    TableResource(db.C,permissions={'list' : GroupOnPermission('b_id.a._id.group')})
    It ensure only users only users of group defined by A table can view items in C
    """

    @property
    def user_id(self):
        return set(self.auth.user_groups)

    @property
    def query(self):
        return self.user_field.belongs(self.user_id)

    def get_groups(self,items):
        ids = map(itemgetter('id'),items)
        return dict((id,(group,)) for id, group in self.sql(self.table._id.belongs(ids),self.table._id,self.user_field,left=self.joins,as_dict=False))

class DeniedPermission(Permission):
    def __call__(self, func):
        def w(*args,**kwargs):
            raise Unauthorized('Permission denied')
        return w

    @property
    def query(self):
        raise Unauthorized('Permission denied')

####### PERMISSION ALGEBRA ##########

class OrPermission(Permission):
    def __init__(self,a,b):
        # untested
        self.a,self.b = a,b

    def __call__(self, func):
        # TESTED and working
        self.resource = func.im_self
        a = self.a(func)
        b = self.b(func)
        self.joins = self.a.joins + self.b.joins
        self.p0 = self.a.p0.union(self.b.p0)
        # if type(method) is not MethodType:
        #     method = MethodType(method, self, type(self))
        # method.im_func.verb = getattr(func, 'verb', False)
        # setattr(resource, func_name, method)

        logger.debug('PM : setting OR permission for %s for %s verb.' % (self.resource.name, func.__name__))

        if any(getattr(x,'id_func',False) for x in (func.im_func,self.a,self.b)):
        #     if getattr(self.a,'id_func',False) and getattr(self.b,'id_func',False):
            if True:
                def w(*args,**kwargs):
                    try:
                        a(*args,**kwargs)
                    except Unauthorized:
                        return b(*args,**kwargs)
                return w
            elif getattr(self.a,'id_func',False):
                return a
            else:
                return b
        return func

    def get_groups(self,items):
        # Untested
        a = self.a.get_groups(items)
        b = self.b.get_groups(items)
        for k,v in b.iteritems():
            if v != a.get(k):
                a[k] = tuple(set(a[k]).union(v))
        return a

    def changed(self,a,b):
        # untested
        return self.a.changed(a,b) or self.b.changed(a,b)

    def get_objects(self,id):
        # untested
        return self.a.get_objects(id) + self.b.get_objects(id)

    @property
    def query(self):
        queries = []
        denied = 0
        try:
            queries.append(self.a.query)
        except Unauthorized:
            denied = 1
        try:
            queries.append(self.b.query)
        except Unauthorized:
            denied += 2
        if denied == 3:
            raise Unauthorized('Permission denied')
        if queries:
            if None in queries:
                return None
            else:
                return reduce(or_,filter(bool,queries))

class AndPermission(Permission):
    def __init__(self,a,b):
        # untested
        self.a,self.b = a,b
        self.joins = a.joins + b.joins
        self.p0 = a.p0.difference(b.p0)

    def __call__(self, func):
        # tested and WORKING
        wb = self.b(func)
        wb.id_func = getattr(func,'id_func',False)
        wb = MethodType(wb,func.im_self,func.im_class)
        wb.im_func.verb = getattr(func, 'verb', False)
        wa = self.a(wb)

        return wa

    def get_groups(self,items):
        # untested
        a = self.a.get_groups(items)
        b = self.b.get_groups(items)
        for k,v in b.iteritems():
            if k in a:
                a[k] = tuple(set(a).intersection(v))
        return a

    def changed(self,a,b):
        # untested
        return self.a.changed(a,b) and self.b.changed(a,b)

    @property
    def query(self):
        a,b = self.a.query, self.b.query
        if a and b:
            return a & b
        elif a:
            return a
        else:
            return b

__all__ = ['MembershipPermission','DelegatePermission','OwnerPermission','DirectPermission','GroupOnPermission']