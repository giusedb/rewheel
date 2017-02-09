__author__ = 'nihil'

from datetime import datetime
from inspect import getargspec

from pydal import SQLCustomType, Field
from types import MethodType

from .exceptions import *
from .main import *
from .permissions import Unauthorized, DelegatePermission, DirectPermission
from .push import *
from .utils import *

mmin = min
mmax = max
empty = set()

class ResourceManager(object):
    _instance = None
    __resources = {}
    __m2m = {}
    _suspended_permissions = {}
    _suspended_traversing = {}
    _suspended_minimals = {}
    _empty = {}
    _synchronized_fields = set()
    _synchronized_memberships = False
    _synchronized_permissions = set()
    _suspended_inv_minimals = {}
    resource = __resources.get
    m2m = __m2m.get

    def __init__(self):
        self._instance = None
        self.__resources = {}
        self.__m2m = {}
        self._suspended_permissions = {}
        self._suspended_traversing = {}
        self._suspended_minimals = {}
        self._empty = {}
        self._synchronized_fields = set()
        self._synchronized_memberships = False
        self._synchronized_permissions = set()
        self._suspended_inv_minimals = {}
        self.resource = self.__resources.get
        self.m2m = self.__m2m.get

    # def __new__(cls, *args, **kwargs):
    #     if not cls._instance:
    #         cls._instance = object.__new__(cls)
    #     return cls._instance

    def __contains__(self, resource):
        """
        Check if a resource is registered in this resource manager
        :param resource: Resource instance
        :return: bool:
        """
        return resource in self.__resources or any(resource in x for x in self._ResourceManager__m2m)

    def get_resources(self):
        return self.__resources.keys()

    def get_empty(self):
        return self._empty.copy()

    def register(self, resource, name=None):
        resource.resource_manager = self
        if not name:
            name = resource.name
        self.__resources[name] = resource
        if hasattr(resource, 'table'):
            self.__resources[resource.table] = resource

        # synching permissions
        if getattr(resource, 'table', None) in self._suspended_permissions:
            resource.all_permissions.update(self._suspended_permissions.pop(resource.table))

        permissions = getattr(resource, 'permissions', False)

        # schema_key = 'PMSchema:%s' % application # Redis key for schema definition

        if permissions:
            # defining minimal permissions for resources
            mp = permissions.get('list')

            if mp:
                # Direct permission case
                if type(mp) is DirectPermission:
                    resource.inv_minimals.add(mp)
                # resource.minimal_permissions.add(mp.permission)
                #     # self.sync_permissions(resource.table._tablename, mp.permission)
                #
                # # Delegate permission
                elif type(mp) is DelegatePermission:
                    res = self.resource(current.db[mp.last_table])
                    if res:
                        res.inv_minimals.add(mp)
                        res.inv_minimals.update(self._suspended_inv_minimals.get(res.table, set()))
                        #         res.minimal_permissions.add(mp.permission)
                        #         minimals = self._suspended_minimals.pop(mp.last_table,set())
                        #         # self.sync_permissions(res.table._tablename, mp.permission)
                        #         res.minimal_permissions.update(minimals)
                    else:
                        self._suspended_inv_minimals.setdefault(mp.last_table, set()).add(mp)
                        #         self._suspended_minimals.setdefault(mp.last_table,set()).add(mp.permission)
                        #
                        #     # defining traversing for minimal permissions
                        #     for table_name, field_name in mp.traversing:
                        #         resource = self.__resources.get(table_name)
                        #         if resource:
                        #             self.sync_db(table_name,field_name)
                        #             resource.traversing.add(field_name)
                        #             fields = self._suspended_traversing.pop(table_name, set())
                        #             for field in resource.traversing.intersection(fields):
                        #                 self.sync_db(table_name,field)
                        #             resource.traversing.update(fields)
                        #         else:
                        #             self._suspended_traversing.setdefault(table_name, set()).add(field_name)
                        # syncyng also last field
                        # if mp.traversing:
                        #     self.sync_db(mp.last_table,mp.traversing)

                        # synchronizing membership if some minimal permission is membership based
                        # elif type(mp) is MembershipPermission:
                        #     # synchronization function to attach on each change to membership definitions
                        #     def _(*args,**kwargs):
                        #         rt_sync_table(current.db.auth_permission,current.db.auth_permission.group_id,primary=current.db.auth_permission.user_id)
                        #     self._synchronized_memberships and _()
                        #     # attaching fake function to each events on membership change
                        #     for x in attrgetter('_after_update','_after_delete','_after_insert')(db.auth_membership):x.append(_)
                        # elif type(mp) is OwnerPermission:
                        #     #TODO : definire cosa serve al sistema di realtime per l'esecuzione dell'ownership
                        #     print 'Ah!!! c\'e\' un Ownership da gestire'

                        # sharing minimal permission to realtime server
                        # serialized = mp.rt_serialize()
                        # from marshal import loads as l
                        # print mp.table._tablename, l(serialized)
                        # if serialized != red.hget(schema_key,name):
                        #     red.hset(schema_key,mp.table._tablename,serialized)
                        #     rt_command('load_permission_schema')

            # if no minimal permission for this table
            # else:
            #     red.hdel(schema_key,name)

            # defining the "all_permissions" attribute for registered resources
            for perm in permissions.itervalues():
                if type(perm) is DelegatePermission:
                    last_resource = self.__resources.get(current.db[perm.last_table])
                    if last_resource:
                        last_resource.all_permissions.add(perm.permission)
                        # last_resource.all_permissions.update(self._suspended_permissions.pop(perm.last_table, set()))
                    else:
                        self._suspended_permissions.setdefault(perm.last_table, set()).add(perm.permission)
                elif type(perm) is DirectPermission:
                    resource.all_permissions.add(perm.permission)

        # if no permission for this table
        # else:
        #     # delete presence of this schema if any
        #     red.hdel(schema_key,name)


        self._empty[name] = dict(updated=[], deleted=[])

    def register_m2m(self, resource):
        resources = tuple(imap(attrgetter('name'), resource.resource_order))
        self.__m2m[resources] = resource
        self.__m2m[tuple(reversed(resources))] = resource
        self.__resources[resource.table] = resource

    def clear(self):
        """
        Clear all registered resources
        :return: None
        """
        self._instance = None
        self.__resources = {}
        self.__m2m = {}
        self._suspended_permissions = {}
        self._suspended_traversing = {}
        self._suspended_minimals = {}
        self._empty = {}
        self._synchronized_fields = set()
        self._synchronized_memberships = False
        self._synchronized_permissions = set()
        self._suspended_inv_minimals = {}


class Resource(object):
    def __init__(self, name, realtime_endpoint=None):
        if getattr(self,'enable_realtime',False):
            self.enable_realtime()

    def get(self, id=None, ids=None):
        raise NotImplemented()

    def list(self, fields=None, filter={}):
        raise NotImplemented()

    def put(self, **kwargs):
        raise NotImplemented()

    def describe(self):
        raise NotImplemented()


def unallowed(*args, **kwargs):
    raise Unauthorized( T('not authorized'))


for func_name in ('get', 'put', 'post', 'delete'):
    setattr(Resource, func_name, unallowed)


def serialize_method(meth):
    args = getargspec(meth)
    # print 'serialize method of %s' % meth.im_self.name, meth.im_func.func_name,args.args[2:]
    return meth.im_func.func_name, args.args[2:]


class PrivateArgs(object):
    """

    """

    def __init__(self, resource, fields):
        """
        Create a Private args Referencee object
        :param resource: TableResource who this PrivateArgs is referred to
        :param fields:
        :return:
        """

        self.db = resource.db
        self.table = self.db.define_table('%s_private' % resource.table._tablename,
                                             Field('refs', resource.table),
                                             Field('auth_user', current.db.auth_user),
                                             *fields)
        if 'mysql' in type(current.db._adapter).__name__.lower():
            try:
                self.db.executesql('alter table %s add constraint unique (refs,auth_user)' % self.table._tablename)
            except:
                pass
        self.select_columns = (self.table.refs,) + tuple(fields)
        self.descibe_columns = fields
        self.resource_name = resource.name

    def list(self, ids, merge_dict=None):
        """
        Get a list of object from ref table and returns final dict to merge with results
        :param ids: list of is from resource table
        :param merge_dict: partial dict to merge results
        :return: {'PA' : {<resource table> { <id> : {param args}}}
        """
        objs = sql(self.db, self.table.refs.belongs(ids) & (self.table.auth_user == self.auth.user_id),
                   *self.select_columns)
        ret = {self.resource_name: dict((x.pop('refs'), x) for x in objs)}
        if merge_dict:
            merge_dict.get('PA', {}).update(ret)
            return merge_dict
        return dict(PA=ret)

    def describe(self):
        """
        Create partial description to attach to main description in order to understand whole resource
        :return: dict of partial description
        """
        return dict((field.name, dict(
            id=field.name,
            name=field.label,
            validators=ValidatorSerializer(
                field.requires if isSequenceType(field.requires) else [field.requires])(),
            comment=field.comment,
            readable=field.readable,
            writable=field.writable,
            type=getattr(field, 'wtype',
                         field.type.type if isinstance(field.type, SQLCustomType) else field.type.split('(')[0]),
            # w2pwidget=field.widget,
        )) for field in self.descibe_columns)

    def save(self, id, **fields):
        """
        Save private attributes to database
        :param id: referent table id
        :param fields: updatable fields dict
        :return: None
        """
        # TODO : Effettuare la validazione prima di inserire
        user_id = current.auth.user_id
        record = self.table(auth_user=user_id, refs=id)
        if record:
            record.update(**fields)
            record.update_record()
        else:
            self.table.insert(auth_user=user_id, refs=id, **fields)

    def delete(self, ids):
        self.db(self.table.refs.belongs(ids)).delete()


class TableResource(Resource):
    doc = None
    base_verbs = 'get', 'put', 'list', 'delete', 'all_perms', 'my_perms', 'describe', 'set_permissions', 'post', 'w', 'savePA'
    minimal_permissions = set()
    zero_permissions = {}
    _description = False

    def compute_virtuals(self, row):
        """
        Compute virtual fields of a dict and update passed dictionary
        :param row: dict representing a row in database
        :return:
        """
        if self.virtual_fields:
            row.update((f, self.table[f].f(row)) for f in self.virtual_fields)

    def __init__(self, app, table_name, fields=None, permissions=None,  field_order=[], caching=None, private_args=[], realtime_endpoint=None, **table_args):
        """
        :param table: dal.Table object to wrap.
        :param permissions: dictionary for permissions {<verb> : [permission1,permission2]}
            each permission could be:
                - a string representing permission name
                - a list, or tuple of:
                    - string representing permission name
                    - referring table (it could be one of following):
                        - dal.Expression join
                        - string representing table name
                        - None : it cascade to current table
                    - id function:
                        id function is a function who resolves the ID of the object on table (previous argument),
                        starting on same signature of verb
                - a pair of ('membership',<group>):
                    something like @auth.has_membership(<group>)
            If You use more then a permission on the same verb, verb function will be called only if ALL permissions are
            satisfied.
        :param private_args: web2py Field list of private args
        """
        self.db = db = app.db
        if type(table_name) is str:
            self.table = table = db.define_table(table_name, *fields, **table_args)
        else:
            self.table = table = table_name

        # dal reference fix
        for field in itemgetter(*self.table._fields)(self.table):
            if type(field.type) is str and field.type.startswith('reference') and not getattr(field, 'referent', False):
                field.referent = table._db[field.type.split(' ')[-1]]._id
                table._references.append(field)

        # collecting date fields
        self.date_fields = set(imap(attrgetter('name'), ifilter(lambda x: x.type in ('date', 'datetime'),
                                                                attrgetter(*table.fields)(table))))
        self.has_permissions = bool(permissions)

        self.inv_minimals = set()
        # self.traversing = set()
        self.all_permissions = set()
        table._resource = self
        self.realtime_enabled = realtime_endpoint != False
        self.table = table
        self.resources_has = {}
        pt = db.auth_permission

        # self.table.__class__ = DynamicTable
        self.__doc__ = self.table._tablename
        self.permissions = permissions
        self.minimal_permission = permissions.get('list') if permissions else None
        # self.permissions = dict((k,tuple((vv,None,None) if type(vv) is str else tuple(vv) + ((None,) * (3 - len(vv))) for vv in v)) for k,v in permissions.items()) if permissions else None
        # print 'permission for %s = ' % self.table._tablename ,set(v if type(v) is str else v[0] for val in permissions.values() for v in val) if permissions else set()
        self.name = self.__doc__ = self.table._tablename
        self.table_args = dict(
            (f.name, f.default) for f in itemgetter(*table._fields)(table) if
            f.writable and f.name and f.default)

        # private args management
        if private_args:
            self.private_args = PrivateArgs(self, private_args)
        else:
            self.private_args = None

        # general references
        self.references_id = dict((tab, (set(f), TableResource.visible_fields(tab))) for tab, f in
                                  groupby(sorted(table._referenced_by, key=attrgetter('_table')), attrgetter('_table')))
        self.references = self.references_id.copy()
        for tab, fields in groupby(table._references, lambda x: x.referent._table):
            self.references.setdefault(tab, (set(), TableResource.visible_fields(tab)))[0].update(fields)

        visible_fields = set(imap(attrgetter('name'), TableResource.visible_fields(table)))
        self.field_order = filter(visible_fields.__contains__, field_order) + [field_name for field_name in
                                                                               map(attrgetter('name'),
                                                                                   TableResource.visible_fields(table))
                                                                               if
                                                                               field_name not in field_order]

        self.visible_fields = itemgetter(*self.field_order)(table)
        # TODO: possibile ottimizzazione e' il raggruppamento anche per campi referenziati (quando una tabella ha piu' campi che puntano alla stessa tabella)


        self.virtual_fields = dict(
            (f.name, f) for f in attrgetter(*dir(table))(table) if type(f) is Field.Virtual).keys()
        self.visible_names = map(attrgetter('name'), self.visible_fields)
        # determining field order
        self.extra_verbs = list(serialize_method(f) for f in attrgetter(*dir(self))(self) if
                                hasattr(f, 'verb') and f.func_name not in self.base_verbs)

        self.sql = partial(sql_decoded if any(hasattr(field.type, 'decoder') for field in self.visible_fields) else sql,self.db)
        if permissions:
            self.zero_permissions = dict((group_id, set(imap(ig1, g))) for group_id, g in groupby(
                self.sql((pt.table_name == self.table._tablename) & (pt.record_id == 0), pt.group_id, pt.name,
                    orderby=pt.group_id, as_dict=False), ig0))
            for func_name, perm in permissions.items():
                func = getattr(self, func_name, None)
                if func:
                    # method = MethodType(check_permissions(func,perms,table),self,type(self))
                    method = perm(func)
                    if type(method) is not MethodType:
                        method = MethodType(method, self, type(self))
                    method.im_func.verb = getattr(func, 'verb', False)
                    setattr(self, func_name, method)
        super(TableResource, self).__init__(table._tablename, realtime_endpoint=realtime_endpoint)
        self.many_to_many = []
        # realtime system permissions synchronization
        # if realtime_enabled and permissions:
        #     sync_permissions(self.table._tablename, names=self.minimal_permissions)

    def enable_realtime(self):
        if not self.realtime_enabled:
            self.realtime_enabled = True
            self.table._after_insert.append(self.rt_insert)
            self.table._after_update.append(self.rt_after_update)
            self.table._before_delete.append(self.rt_delete)
            if self.has_permissions:
                self.table._before_update.append(self.rt_before_update)

    @verb
    def savePA(self, id, **fields):
        if self.private_args:
            return self.private_args.save(id, **fields)

    @call_back(mk_realtime_buffer)
    def rt_insert(self, row, id):
        # lst = current.log_update.setdefault(self.name, {}).setdefault('results', [])
        # lst.append(sql(self.table._id == id, *self.visible_fields)[0])

        # new version
        verb = 'inserted' if self.minimal_permission else 'results'
        row = self.sql(self.table._id == id, *self.visible_fields)
        if row:
            row = row[0]
            current.update_log.append([verb, self, row])

    @call_back(mk_realtime_buffer)
    def rt_before_update(self, selection, arg_dict):
        if not hasattr(current, 'update_idx'):
            current.update_idx = {}
        idx_update = current.update_idx.setdefault(self.name, {})
        fid = self.table._id.name
        idx_update.update((x[fid], x) for x in self.sql(selection, *self.visible_fields))

    @call_back(mk_realtime_buffer)
    def rt_after_update(self, selection, arg_dict):
        # only for permission governed tables
        # if self.permissions:
        #     fid = self.table._id.name
        #     idx_update = current.update_idx[self.name]
        #     lst = current.log_update.setdefault(self.name, {}).setdefault('update', [])
        #     for row in sql(selection, *self.visible_fields):
        #         lst.append((idx_update[row[fid]], row))
        # # for non permission governed tables
        # else:
        #     lst = current.log_update.setdefault(self.name, {}).setdefault('results', [])
        #     lst.extend(sql(selection, *self.visible_fields))

        # new version
        if self.minimal_permission:
            idx_update = current.update_idx[self.name]
            current.update_log.extend(
                [['update', self, [idx_update[row['id']], row]] for row in self.sql(selection, *self.visible_fields)])
        else:
            current.update_log.extend([['results', self, row] for row in self.sql(selection, *self.visible_fields)])

    @call_back(mk_realtime_buffer)
    def rt_delete(self, selection):
        rows = column(self.db, selection, self.table._id)
        # if rows:
        #     lst = current.log_update.setdefault(self.name, {}).setdefault('deleted', {}).setdefault('id',set())
        #     lst.update(rows)
        # new version
        current.update_log.extend([['deleted', self, x] for x in rows])

    @classmethod
    def visible_fields(cls, tab):
        # TODO : Aggiungere i VirtualFields
        return tuple(field for field in attrgetter(*tab.fields)(tab) if
                     (field.readable or field.writable) and field.type != 'password')

    def CASQuery(self, query=None, verb=None):
        join = None
        q = query
        if self.permissions:
            permission = self.permissions.get(verb)
            if permission:
                if permission.p0.intersection(self.app.auth.user_groups):
                    query = q
                else:
                    query = permission.query
                if q or query:
                    q = reduce(and_, filter(bool, (query, q)))
                join = permission.joins
        return q, join

    @verb
    def my_perms(self, ids, **kwargs):
        """
        find all permissions to a list of objects and returns current logged user permission on that object
        :param ids: list, tuple or set of id you want to chek permission on
        :param kwargs: unused
        :return: dictionary like {<object id> : [<permission_name>,<permission_name>,...]}
        """
        auth = self.app.auth
        # checking all objects
        p = self.db.auth_permission
        if type(ids) in (list, tuple, set):
            _ids = type(ids)((0,)) + ids
        else:
            _ids = [0, ids]
        grouped = self.db(p.record_id.belongs(_ids) & p.group_id.belongs(auth.user_groups.keys()) & (
            p.table_name == self.table._tablename)).select(p.name, p.record_id).group_by_value('record_id')
        take_names = itemgetter('name')
        base_permissions = set(imap(take_names, grouped.get(0, set())))
        ret = dict(PERMISSIONS={self.name: [
            dict((id, set(imap(take_names, grouped.get(id, []))).union(base_permissions)) for id in map(int, ids))]})
        current.response.text = ret
        return ret

    @verb
    def all_perms(self, id, **kwargs):
        """
        Restituisce tutti i permissi di un determinato oggetto
        """
        p = self.db.auth_permission
        if self.all_permissions:
            ret = self.sql(
                (p.record_id == id) & (p.table_name == self.table._tablename) & p.name.belongs(self.all_permissions),
                p.name, p.group_id,
                orderby=p.group_id)
        else:
            ret = []
        current.response.text = ret
        return ret

    def get_references(self, objects, id=False):
        # getting refereced objects
        if id:
            references = self.references_id
        else:
            references = self.references
        ret = {}
        for table, (fields, table_fields) in references.items():
            selection = tuple(field.belongs(
                objects.column(field.referent.name)) if field._table != self.table else field.referent.belongs(
                objects.column(field)) for field in fields)
            results = self.db(reduce(or_, selection)).select(*((table._id,) if id else table_fields))
            ret[table._tablename] = dict(results=results, totalResults=len(results))
            print self.db._lastsql
        return ret

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self.name)

    @verb
    def list(self, filter=None, _check_permissions=True, together='', _jsoned=True):
        """
        Fetch a list of resource and return generic description of items
        """

        join = None
        if filter:
            for k, v in filter.iteritems():
                if None in v:
                    filter[k] = None
            query = reduce(and_,
                           (self.table[field].belongs(value) if type(value) is list else (self.table[field] == value)
                            for field, value in filter.iteritems()))
        else:
            query = None
        if _check_permissions:
            query, join = self.CASQuery(query, verb='list')
        fields = (self.table.ALL,) if self.virtual_fields else self.visible_fields
        # objects = self.db.executesql(self.db(query)._select(*fields,join=join),as_dict=True)
        objects = self.sql(query, *fields, left=join, as_dict=self.virtual_fields)
        if self.virtual_fields and objects:
            # calcolo tutti i virtual fields
            for obj, field in product(objects, [self.table[field] for field in self.virtual_fields]):
                obj[field.name] = field.f(obj)

            vn = partial(zip, self.visible_names + self.virtual_fields)
            get_vn = itemgetter(*(self.visible_names + self.virtual_fields))
            objects = map(dict, map(vn, map(get_vn, objects)))
            # print objects
        ret = {self.name: dict(results=objects, totalResults=len(objects), )}
        if together:
            if 'permissions' in together:
                ret.setdefault('PERMISSIONS', {}).update(
                    self.my_perms(ids=map(itemgetter('id'), objects)).get('PERMISSIONS', {}))

            # results = {self.name : objects}
            for resource, redest, t, field in self.find_model_path(together):
                # print resource, redest, field,t
                if t == 'm':
                    if resource in ret:
                        obs = map(itemgetter('id'), ret[resource]['results'])
                        ret.setdefault('TOMANY', {})['%s_%s' % (redest, field.name)] = obs
                        if obs:
                            ret.update(resource_manager.resource(redest).list(filter={field.name: obs}, _jsoned=False))
                elif t == '1':
                    if resource in ret:
                        obs = list(set(map(itemgetter(field.name), ret[resource]['results'])))
                        # ret.setdefault('TOONE',{})['%s_%s' % (resource,field.name)] = obs
                        if obs:
                            ret.update(resource_manager.resource(redest).list(filter={'id': obs}, _jsoned=False))
                elif t == 'M':
                    if resource in ret:
                        first = 0 if field else 1
                        m2m_idx = '%s/%s|%s' % (resource, redest, first)
                        obs = map(itemgetter('id'), ret[resource]['results'])
                        ret.setdefault('MANYTOMANY', {}).setdefault(m2m_idx, []).extend(obs)
                        if obs:
                            resource_manager.m2m((resource, redest)).list(resource_manager.resource(redest),
                                                                          collection=obs)
                            res = current.response.text
                            ret.setdefault('m2m', {}).update(res['m2m'])
                            obs = list(set(map(itemgetter(1 - first), imap(itemgetter('add'), res['m2m'][m2m_idx]))))
                            # ret.setdefault('TOMANY',{})[redest] = obs
                            if obs:
                                res = resource_manager.resource(redest).list(filter=dict(id=obs), _jsoned=False)
                                ret.update(res)
        if self.private_args:
            if objects:
                ret.update(self.private_args.list(map(itemgetter(self.field_order.index('id')), objects)))

        current.response.text = ret
        return ret

    @cached
    def find_model_path(self, together):
        together = together.split(',')
        tables = [self.table]
        got = []
        for resource in together:
            found = False
            for tab in tables:
                if not found:
                    tipo = None
                    m2m = dict(map(itemgetter('model', 'first'), tab._resource.many_to_many))
                    # m2m = set(map(itemgetter('model'),tab._resource.many_to_many))
                    o2m = dict(
                        (field._table._resource.name, field) for field in tab._referenced_by if resource not in m2m)
                    m2o = dict((field.referent._table._resource.name, field) for field in tab._references if
                               resource not in m2m)

                    # many to one
                    if resource in m2o:
                        # print 'one to many %s,' % resource
                        tipo = '1'
                        field = m2o[resource]

                    # one to many
                    elif resource in o2m:
                        # print 'many to one %s.' % resource
                        tipo = 'm'
                        field = o2m[resource]

                    # many to many
                    elif resource in m2m:
                        # print 'many to many %s' % resource
                        tipo = 'M'
                        # field = resource_manager.m2m((self.name,resource)).table[resource + '_id']
                        field = m2m[resource]
                    if tipo:
                        got.insert(0, [tab._resource.name, resource, tipo, field])
                        tables.append(resource_manager.resource(resource).table)
                        found = True
        return got

    def fix_dates(self, row):
        """
        translate angular JS format to datetime
        :param row: dict row as arrived from request
        :return: None
        """
        for field in self.date_fields:
            if field in row:
                if not type(row[field]) is datetime:
                    try:
                        row[field] = datetime.fromtimestamp(float(row[field]))
                    except Exception as e:
                        row[field] = None

    def validate(self, obj, id=None, isNew=False):
        # input validation
        args = {}
        if isNew:
            args = dict((field, None) for field in self.table._fields)
            del args['id']
        args.update(self.table_args.copy())
        args.update(obj)

        # clean all non field attribute
        for arg in tuple(ifilterfalse(self.table._fields.__contains__,args)):
            del args[arg]

        id = id or args.get('id')
        self.fix_dates(args)
        # self.table._db = self.db

        # validate arguments and build error dict
        errors = {}
        for field, value in args.iteritems():
            args[field], errors[field] = self.table[field].validate(value, id)
        # clean null error
        return args, dict((k, v) for k,v in errors.iteritems() if v)

    @verb
    def put(self, multiple=None, _check_permissions=True, _base_permissions=True, formIdx = None, **kwargs):
        """
        Implements new item inserted
        :param multiple:
        :param _check_permissions:
        :param _base_permissions:
        :param kwargs:
        :return: int: object id
        """
        if multiple:
            # TODO effettuare prima la validazione di tutti e poi effettuare l'inserimento di tutti
            # TODO anche il client devra' essere in grado di gestire l'inserimento multiplo considerando anche piu' formIdx
            # validating all
            objs, errors = tuple(izip(*imap(self.validate, multiple)))
            if any(errors):
                raise ValidationError(errors, self.name)
            ids = []
            for obj in objs:
                if obj.get('id'):
                    ids.append(obj.get('id'))
                    self.db(self.table._id == obj.pop('id')).update(**obj)
                else:
                    ids.append(self.table.insert(**obj))
            self.list(filter={'id': ids}, _check_permissions=False)
            return ids
        args, errors = self.validate(kwargs, isNew=True)
        if errors:
            raise ValidationError(errors, self.name)
        else:
            if 'id' in args:
                self.table.insert(**args)
                self.rt_insert(args, args['id'])
                id = args['id']
            else:
                id = self.table.insert(**args)['id']
            if self.all_permissions and _base_permissions:
                auth = current.auth
                group_id = auth.id_group('user_%s' % auth.user_id)
                # granting all permission to creator
                self.set_permissions(id, {group_id: self.all_permissions}, _check_permissions=False)
        current.response.text = str(id)
        return id

    @put.func_id
    def id_func(*args, **kwargs):
        return kwargs.get('id')

    @verb
    def post(self, **kwargs):
        """
        Implements updating of an exeisting record
        :param kwargs:
        :return:
        """
        # args = kwargs.copy()
        id = kwargs.pop('id', None)
        self.fix_dates(kwargs)

        kwargs, errors = self.validate(kwargs, id)

        if errors:
            raise ValidationError(errors, self.name)
        elif kwargs:
            self.db(self.table.id == id).update(**kwargs)
        return id

    @post.func_id
    def func_id(args, kwargs):
        return kwargs.get('id')

    @verb
    # @jsoncached
    def describe(self):
        # TODO TableResource.describe() can be cached
        if not self._description:
            model = self.table
            m2m = set(map(itemgetter('model'), self.many_to_many)).union(
                imap(lambda x: x.replace('/', '_'), imap(itemgetter('indexName'), self.many_to_many)))
            # getting all interesting fields
            fields = tuple(field for field in attrgetter(*model.fields)(model) if
                           (field.readable or field.writable) and not (
                           not isinstance(field.type, SQLCustomType) and field.type.startswith('reference')))

            ret = {model._tablename: dict(
                fields=dict((field.name, dict(
                    name=field.label,
                    validators=ValidatorSerializer(
                        field.requires if isSequenceType(field.requires) else [field.requires])(),
                    comment=field.comment,
                    readable=field.readable,
                    writable=field.writable,
                    type=getattr(field, 'wtype',
                                 field.type.type if isinstance(field.type, SQLCustomType) else field.type.split('(')[
                                     0]),
                )) for field in fields),
                doc=self.doc or self.__doc__,
                representation=re_format_fields.findall(model._format) if model._format else [],
                name=self.table._tablename,
                references=tuple(
                    dict(
                        name=field.label,
                        comment=field.comment,
                        readable=field.readable,
                        writable=field.writable,
                        to=field.referent.table._tablename,
                        id=field.name,
                    )
                    for field in model._references if
                        field.referent.table._tablename not in m2m and (field.readable or field.writable) and field.referent in self.resource_manager
                ),
                referencedBy=tuple(
                    dict(
                        name=field.label,
                        comment=field.comment,
                        readable=field.readable,
                        writable=field.writable,
                        by=field.table._tablename,
                        id=field.name,
                    )
                    for field in model._referenced_by if
                        field.table._tablename not in m2m and (field.readable or field.writable) and field.referent in self.resource_manager
                ),
                fieldOrder=self.field_order,
                extra_verbs=self.extra_verbs,
                permissions=self.all_permissions,
                manyToMany=self.many_to_many
            )}
            if self.private_args:
                ret[model._tablename]['privateArgs'] = self.private_args.describe()
            for field in ifilter(attrgetter('default'), fields):
                if not isCallable(field.default):
                    ret[model._tablename]['fields'][field.name].setdefault('options', {})['default'] = field.default
            # add custom widgets
            for field in ifilter(attrgetter('widget'),fields):
                ret[model._tablename]['fields'][field.name]['widget'] = field.widget
            self._description = ret
        ret = self._description
        # adding default values
        current.response.text = ret
        return ret

    @verb
    @call_back(mk_realtime_buffer)
    def set_permissions(self, id, permissions, _check_permissions=True):
        """
        garantisce i permessi indicati sull'oggetto e revoca tutti i permessi non espressamente contenuti
        in "permissions"
        :param id: id dell'oggetto sul quale si vogliono modificare i permessi
        :param permissions: dizionario dei permessi {group_id, [<permesso1>,<permesso2>,..]}
        """
        auth = self.app.auth
        pt = self.db.auth_permission
        actual_permissions = dict((gid, set(imap(ig1, g))) for gid, g in groupby(
            self.sql((pt.record_id == id) & (pt.table_name == self.table._tablename), pt.group_id, pt.name, as_dict=False,
                orderby=pt.group_id), ig0))
        minimals = set(map(attrgetter('permission'), self.inv_minimals))
        permissions = dict((int(k), set(x for x, y in p.iteritems() if y) if type(p) is dict else p) for k, p in
                           permissions.iteritems())

        changed_groups = set()
        minimal_changed = {}
        for group_id, perms in permissions.items():
            for perm_name in self.all_permissions:
                val = perm_name in perms
                if val != (perm_name in actual_permissions.get(group_id, empty)):
                    (auth.add_permission if val else auth.del_permission)(
                        group_id=group_id,
                        name=perm_name,
                        table_name=self.table._tablename,
                        record_id=id,
                    )
                    changed_groups.add(group_id)
                    if perm_name in minimals:
                        minimal_changed.setdefault(perm_name, {})[group_id] = val

        minimal_permissions = dict((p, set()) for p in minimal_changed)
        for p in (p for p in self.inv_minimals if p.permission in minimal_permissions):
            minimal_permissions[p.permission].add(p)

        if self.realtime_enabled:
            related_objects = {}
            if minimal_changed:
                for perm_name, groups in minimal_changed.iteritems():
                    rel_perm = {}
                    for permission in minimal_permissions[perm_name]:
                        related = permission.get_objects(id)
                        if related:
                            rel_perm.setdefault(permission.resource, []).extend(related)
                    if perm_name not in related_objects:
                        related_objects[perm_name] = {}
                    related_objects[perm_name] = rel_perm

            if reduce(set.union, related_objects.itervalues(), set()):
                delete_message = {}
                insert_message = {}
                for perm, rel_objs in related_objects.iteritems():
                    del_message = delete_message.get(perm, [])
                    ins_message = insert_message.get(perm, [])
                    for resource, items in rel_objs.iteritems():
                        del_message.extend([[resource.name, 'deleted', id] for id in imap(itemgetter('id'), items)])
                        ins_message.extend([[resource.name, 'results', item] for item in items])
                    delete_message[perm] = del_message
                    insert_message[perm] = ins_message

                # send realtime XOR
                for perm_name in minimal_permissions:
                    positive_groups = set(k for k, p in permissions.iteritems() if perm_name in p)
                    negative_groups = set(k for k, p in permissions.iteritems() if perm_name not in p)
                    # if positive_groups and negative_groups:
                    current.rt_permissions.append(('send_xor', self.app, positive_groups, negative_groups,
                                                   insert_message[perm_name], delete_message[perm_name]))
                    # elif positive_groups:
                    #     rt_command('send_groups',insert_message[perm_name],map(int,positive_groups))
                    # else:
                    #     rt_command('send_groups',delete_message[perm_name],map(int,negative_groups))

            for g, p in self.zero_permissions.iteritems():
                actual_permissions.setdefault(g, set()).update(p)
                permissions.setdefault(g, set()).update(p)

            if id in map(itemgetter('id'),
                         imap(ig2, ifilter(lambda x: x[0] == 'inserted' and x[1] == self, current.update_log))):
                actual_permissions = {}

            current.rt_permissions.append(['send_permissions',self.app, self.name, id, permissions, actual_permissions])
            # rt_command('send_permissions',self.name,id,permissions,actual_permissions)

        # self.db.commit()
        ret = self.my_perms([id])
        current.response.text = ret
        return ret

    @verb
    def delete(self, id=None, **kwargs):
        """delete object and recursively all related."""
        rm = ResourceManager()
        pt = self.db.auth_permission
        if id and not isinstance(id, (list, tuple, set)):
            id = [id]

        # removing private args
        if self.private_args:
            private_args = self.private_args.table
            self.private_args.delete(id)
        else:
            private_args = None

        # # removing many to many references
        # m2ms = set()
        # for reference in (tuple(x.split('/')) for x in imap(itemgetter('indexName'),self.many_to_many)):
        #     resource = rm.m2m(reference)
        #     if resource:
        #         m2ms.add(resource.table)
        #         resource.delete(self,collection = id)

        # getting table names and field names to delete
        cascading_deletion = tuple((field.table, field) for field in self.table._referenced_by if
                                   field.ondelete == 'CASCADE' and field.table != private_args)  # and field.table not in m2ms)
        # deleting all related objects
        for table, field in cascading_deletion:
            res = rm.resource(table)
            if res:
                # fetch all id of related rows
                ids = set(chain(*self.sql(field.belongs(id), table._id, as_dict=False)))
                if ids:
                    # if related entitiy is a many to many relation delete reference with other objects, but not related objects
                    if isinstance(res, ManyToManyRelation):
                        # making deletion simpy by forign related attribute
                        res.delete(self, resource_id=ids)
                    else:
                        res.delete(id=ids, _check_permissions=False)

        self.db(self.table.id.belongs(id)).delete()
        # deleting all directly related permissions
        self.db((pt.table_name == self.table._tablename) & pt.record_id.belongs(id)).delete()
        # if realtime_enabled and self.minimal_permissions:
        #     sync_permissions(self.table._tablename, id, self.minimal_permissions)
        #     perms = sql(pt.record_id.belongs(id) & (pt.table_name == self.table._tablename))
        #     if perms:
        #         rt_sync_permissions(self.table, id, perms)

    @delete.func_id
    def get_id(*args, **kwargs):
        return args[0] if args else kwargs.get('id')


TableResource.put.im_func.id_func = lambda a, b: b.get('id')


class MasterResource(Resource):
    """
    Manage all resources
    """

    def __init__(self):
        self.name = 'Resource'
        super(MasterResource, self).__init__('resources')

    def get(self, resource_name):
        # getting resource
        resource = ResourceManager().resource(resource_name)
        if resource:
            # build return dictionary
            return dict(
                name=resource.name,
                doc=resource.__doc__,
                methods=tuple(
                    dict(
                        name=method_name,
                        doc=method.__doc__,
                    )
                    for method_name, method in resource.__dict__.iteritems() if isCallable(method)
                )
            )
        raise HTTP(404, 'resource known as %s not found on this server' % resource_name)

    @verb
    def list(self):
        current.response.text = filter(lambda x: type(x) is str, ResourceManager().get_resources())
        return current.response.text


class ManyToManyRelation(object):
    """
    Creates a many to many relation between two different resources.
    This class will create relation table defining new relation named <table1>_<table2> with (id,<table1>_pk,<table2>_pk)
    fields
    """
    allowed_methods = set(('list', 'put', 'delete'))

    def __init__(self, resource1, resource2, connection_table=None, fields=None, permissions=None, left_has=None,
                 right_has=None):
        """
        :param resource1: TableResource instance 1
        :param resource2: TableResource instance 2
        :param connection_table: table in which there is a a field who links an item from resource1 and an item from resource2
                            If not supplied it will be defined as <table1>_<table2>
        :param fields: web2py fields pair for [field references resource1, field references resource2]
        :param permissions: not yet thunk
        :return:
        """
        resources = (resource1, resource2)
        tables = map(attrgetter('table'), resources)
        names = tuple(imap(itemgetter('_tablename'), tables))

        # has_references

        self.left_has = left_has and (tables[0], left_has)
        self.right_has = right_has and (tables[1], right_has)
        if self.left_has:
            resource1.resources_has[left_has] = self
        if self.right_has:
            resource2.resources_has[right_has] = self

        self.js_name = '/'.join(names)
        self.db = resource1.db
        self.resource1 = resource1
        self.table_name = connection_table._tablename if connection_table else '%s_%s' % names
        if fields:
            resource_fields = tuple(izip((resource1, resource2), fields))
        else:
            resource_fields = tuple(
                (resource, Field('%s_id' % table, resource.table, ondelete='CASCADE')) for resource, table in
                zip(resources, names))
        self.rev_fields = dict(zip(map(itemgetter(0), resource_fields), reversed(map(itemgetter(1), resource_fields))))
        self.resource_fields = dict(resource_fields)
        self.table = connection_table or self.db.define_table(self.table_name,
                                                              *imap(self.resource_fields.get, resources))
        self.table._resource = self
        self.name = self.table_name
        self.realtime_enabled = resource1.realtime_enabled or resource2.realtime_enabled
        self.resource_order = {
            resource2: lambda x: (x[1], x[0]),
            resource1: lambda x: x,
        }
        self.indexName = '%s/%s' % tuple(imap(attrgetter('name'), (resource1, resource2)))
        self.first = lambda r: 0 if r != self.resource1 else 1
        for resource, other, name in izip(reversed(resources), resources, names):
            name = name + 's'
            # resource.extra_verbs.append((name, ['verb', 'ids']))
            if not (resource == resource1, self.js_name) in map(itemgetter('first', 'indexName'),
                                                                resource.many_to_many):
                resource.many_to_many.append(dict(
                    model=other.name,
                    indexName=self.js_name,
                    first=resource == resource1,
                ))
            setattr(resource, name, verb(partial(self.call_relation, other)))
        self.names = tuple(n[1].name for n in resource_fields)
        if 'mysql' in type(self.db._adapter).__name__.lower():
            try:
                self.db.executesql(
                    'alter table %s add constraint unique (%s_id,%s_id)' % (self.table_name, names[0], names[1]))
            except:
                pass
        # attaching left_has and right_has to master resource
        ResourceManager().register_m2m(self)

    def call_relation(self, relation, method, *args, **kwargs):
        if method in ManyToManyRelation.allowed_methods:
            return getattr(self, method)(relation, *args, **kwargs)
        raise HTTP(405, 'method not allowed')

    def field_order(self, resource):
        return tuple(reversed(self.names)) if resource is self.resource1 else self.names

    def list(self, resource, collection, withreferences=False):
        # print 'list', resource.name, collection
        # c1,c2 = self.field_order(resource)
        if type(resource) is str:
            resource = resource_manager.resource(resource)
        c2, c1 = self.names
        # if withreferences:
        #     # TODO implementare la possibilita' di chiedere direttamente gli oggetti collegati.
        #     return []
        ret = sql(self.db,self.rev_fields[resource].belongs(collection), c1,c2,orderby=c1,as_dict=False)
        current.response.text = {'m2m': {self.indexName : [{'add': x} for x in ret]}}
        return ret
        # return map(itemgetter(*self.names),self.db(self.rev_fields[resource].belongs(collection)).select())

    def __repr__(self):
        return 'M2M %s' % self.name

    @call_back(mk_realtime_buffer)
    def put(self, resource, collection):
        # print 'put',resource.name
        order = self.field_order(resource)
        rt_buff = [['m2m', self.js_name, {'add': map(int, self.resource_order[resource](x))}] for x in collection]
        relations = tuple(dict(zip(order, item)) for item in collection)
        for r in relations:
            try:
                self.table.insert(**r)
            except Exception as e:
                args = getattr(e, 'args', None)
                if not (args and args[0] and args[0] == 1062):
                    print 'Error inserting many to many %s : %s' % (self.table_name, r)
        if self.realtime_enabled:
            for idx, had in enumerate((self.left_has, self.right_has)):
                if had:
                    table, field_name = had
                    self.db(table.id.belongs(set(imap(itemgetter(idx), collection)))).update(**{field_name: True})
            current.update_log.extend(rt_buff)
        return rt_buff
        # map(current.log_update.setdefault('m2m', {}).setdefault(self.js_name, {}).setdefault('add', []).append, rt_buff)
        # cache invalidation
        # hkeys = ':'.join((current.request.application, order[0][:-3], order[1][:-3] + 's')), ':'.join(
        #     (current.request.application, order[1][:-3], order[0][:-3] + 's'))
        # skeys = map(red.hkeys, hkeys)
        # for lst, hkey, skey in zip(zip(*reversed(collection)), hkeys, skeys):
        #     ret = tuple(k for k, x in zip(skey, (set(map(int, x.split(':'))) for x in skey)) if x.intersection(lst))
        #     if ret:
        #         red.hdel(hkey, *ret)

    @call_back(mk_realtime_buffer)
    def delete(self, resource=None, collection=None, id=None, resource_id=None, _check_permissions=None):
        if id or resource_id:
            if id:
                recs = self.db(self.table.id.belongs(id))
            elif resource_id:
                recs = self.db(self.table[resource.table._tablename + '_id'].belongs(resource_id))
            rt_buff = [['m2m', self.js_name, {'del': x}] for x in imap(itemgetter(*self.table._fields[1:]), sql(self.db,recs))]
            recs.delete()
        else:
            order = self.field_order(resource)
            rt_buff = [['m2m', self.js_name, {'del': self.resource_order[resource](x)}] for x in collection]
            relations = tuple(dict(zip(order, item)) for item in collection)
            for SQL in ('delete from %s where %s' % (
                    self.table._tablename, ' and '.join('%s = %s' % (k, v) for k, v in relation.items())) for relation
                        in
                        relations):
                try:
                    self.db.executesql(SQL)
                except Exception as e:
                    print '%s SQL ERROR : %s' % (e, SQL)
        for idx, had in enumerate((self.left_has, self.right_has)):
            if had:
                table, field_name = had
                if collection:
                    items = set(imap(itemgetter(idx), collection))
                    if items:
                        res = items.difference(set(imap(ig0, current.db.executesql(
                            'select %s, count(*) from %s where %s in (%s) group by %s' % (
                            order[idx], self.table_name, order[idx], ','.join(imap(str, items)), order[idx])))))
                else:
                    res = set(id) if id else None
                if res:
                    self.db(table.id.belongs(res)).update(**{field_name: False})

        if self.realtime_enabled:
            current.update_log.extend(rt_buff)
        return rt_buff

    def enable_realtime(self):
        self.realtime_enabled = True

class ValidatorSerializer:
    # TODO: completare i validatori
    def __init__(self, validators):

        self._serializers = {}

        for validator in validators:
            validator_name = type(validator).__name__
            validator_method = getattr(self, 'serialize_%s' % validator_name, None)
            if validator_method:
                self._serializers.update(validator_method(validator, **self._serializers))

    def serialize_IS_LENGTH(self, validator, minlength=0, maxlength=2 ** 32, **kwargs):
        return dict(
            minlength=max(minlength, validator.minsize),
            maxlength=min(maxlength, validator.maxsize),
        )

    def serialize_IS_INT_IN_RANGE(self, validator, min=0, max=2 ** 32, **kwargs):
        return dict(
            min=mmax(min, validator.minimum),
            max=mmin(max, validator.maximum),
        )

    def serialize_IS_NOT_EMPTY(self, validator, required=False, **kwargs):
        return dict(required=True)

    def serialize_IS_MATCH(self, validator, pattern='', **kwargs):
        return dict(pattern=('(%s)|(%s)' % (regex2js(validator.regex.pattern), pattern)) if pattern else regex2js(
            validator.regex.pattern))

    def serialize_IS_ALPHANUMERIC(self, validator, pattern='', **kwargs):
        return self.serialize_IS_MATCH(validator, pattern)

    def serialize_IS_IN_DB(self, validator, reference=''):
        return dict(reference=validator.label)

    def serialize_IS_IN_SET(self, validator, **kwargs):
        return dict(valid=validator.options()[1:])

    def __call__(self):
        return self._serializers


def regex2js(pattern):
    """
    translate a python regex to javascript regex
    :param pattern: string: Pyhton regex patter
    :return: string: javascript regex pattern
    """
    # TODO: costruire un buon risolutore di espressioni regolari per javascript
    return pattern


master_resource = MasterResource()
resource_manager = ResourceManager()


# web2py realtime system injection
# DAL.__init__ = call_back(mk_realtime_buffer)(DAL.__init__)
# try:
#     current.db._adapter.__class__.commit = call_back(realtime_commit)(current.db._adapter.__class__.commit)
# except:
#     DAL.commit = call_back(realtime_commit)(DAL.commit)
# DAL.rollback = call_back(mk_realtime_buffer)
