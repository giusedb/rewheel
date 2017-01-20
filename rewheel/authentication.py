import os
from rewheel.push import share_user
from .validators import *
from .utils import Storage, sql, column
from uuid import uuid4
from flask import session, redirect, request
from pydal.objects import Set, Query
from logging import getLogger
from .utils import current
from .base import TableResource, verb
from .dalfix import Field

log = getLogger('rewheel.authentication')

AUTHS = {}
user_cache = {} # {id : user db instace}

is_not_empty = IS_NOT_EMPTY()

class UserResource(TableResource):
    """
    User resource definition
    """
    copy_email = False
    @verb
    def put(self, multiple=None, _check_permissions=True, _base_permissions=True, formIdx = None, **kwargs):
        """
        Estends standard put to copy email as username if auth options.use_username is False
        :param multiple:
        :param _check_permissions:
        :param _base_permissions:
        :param formIdx:
        :param kwargs:
        :return:
        """
        if self.copy_email:
            kwargs['username'] = kwargs['email']
        super(UserResource,self).put(multiple=multiple, _check_permissions=_check_permissions,_base_permissions=_base_permissions,formIdx=formIdx, **kwargs)


class Auth(object):
    """
    reWheel Authentication object class (taken by web2py)
    """
    _default_messages = Storage(
        label_first_name = 'Name',
        label_last_name = 'Family name',
        label_email = 'e-mail',
        label_reset_password = 'Password reset',
        invalid_email = 'invalid email',
        email_taken = 'This email is registered yet',
    )

    _default_settings = dict(

    )
    _tables = lambda self, db: dict(
        auth_user = (
            Field('first_name', length=128, default='',
                  label=self.messages.label_first_name,
                  requires=is_not_empty),
            Field('last_name', length=128, default='',
                  label=self.messages.label_last_name,
                  requires=is_not_empty),
            Field('email', length=512, default='',
                  label=self.messages.label_email,
                  requires=[
                        IS_EMAIL(error_message=self.messages.invalid_email),
                        IS_NOT_IN_DB(db, 'auth_user.email', error_message=self.messages.email_taken),
                        IS_LOWER(),
                  ]),
            Field('username', length=128, default='',
                  label=self.messages.label_username,
                  requires=[
                      IS_MATCH('[\w\.\-]+', strict=True, error_message=self.messages.invalid_username),
                     IS_NOT_IN_DB(db, 'auth_user.username', error_message=self.messages.username_taken)]),
            Field('password', 'password', length=512,
                  readable=False, label=self.messages.label_password,
                  requires=MD5()),
            Field('registration_key', length=512,
                  writable=False, readable=False, default='',
                  label=self.messages.label_registration_key),
            Field('reset_password_key', length=512,
                  writable=False, readable=False, default='',
                  label=self.messages.label_reset_password_key),
            Field('registration_id', length=512,
                  writable=False, readable=False, default='',
                  label=self.messages.label_registration_id),
        ),
        auth_group = (
            Field('role', length=50, default='',
                  label=self.messages.label_role,
                  requires=IS_NOT_IN_DB(db, 'auth_group.role')),
            Field('description', length=200,
                  label=self.messages.label_description),
        ),
        auth_membership = (
            Field('user_id', 'reference auth_user',
                  label=self.messages.label_user_id),
            Field('group_id', 'reference auth_group',
                  label=self.messages.label_group_id),
        ),
        auth_permission = (
            Field('group_id', 'reference auth_group',
                  label=self.messages.label_group_id),
            Field('name', default='default', length=512,
                  label=self.messages.label_name,
                  requires=is_not_empty),
            Field('table_name', length=512,
                  label=self.messages.label_table_name),
            Field('record_id', 'integer', default=0,
                  label=self.messages.label_record_id,
                  requires=IS_INT_IN_RANGE(0, 10 ** 9)),
        ),
    )

    def __init__(self,rewheel_app ):
        """
        Initalize reWheel authentication object
        :param rewheel_app: ReWheelApplication: rewheel application instance
        :param messages: dict: messages for authentication errors
        :param config: dict: configuration arguments
        """
        log.info('creating authentication for %s app' % rewheel_app.name)
        # initialization
        self.app = rewheel_app

        self.get_next = ''
        self.login_url = ''
        self.settings = Storage(self._default_settings)
        self.messages = Storage(self._default_messages)

    def define_tables(self, db, username=False, extra_fields = {}, signature = None):
        """
        Define auth tables on current db
        :param db:
        :param username:
        :param extra_fields:
        :param signature:
        :return:
        """
        self.db = db
        log.debug('defining auth tables')
        messages = Storage(self._default_messages)
        messages.update(self.messages)
        tabs = self._tables(db)
        for table in ('auth_user','auth_group','auth_membership','auth_permission'):
            fields = tabs[table]
            log.debug('defining %s table' % table)
            db.define_table(table, *(fields + extra_fields.get(table,())))
        return self

    def login(self, username, password):
        """
        do login for a given user by it's username and password
        :param username: str: user email, but if "use_username" in settings, it has to be username
        :param password: str: password
        :return: sid, user id
        """
        log.info('%s is trying to login with password ****' % username)
        db = self.db
        am = db.auth_membership
        ag = db.auth_group

        # determining field for validation (username or email)
        password = str(db.auth_user.password.validate(str(password))[0])

        # getting user if exixsts
        user = db.auth_user(password = password, username = username)
        if not user:
            log.info('user %s is unknown ' % username)
            return None, None
        log.info('user %(first_name)s %(last_name)s is accepted' % user)
        session['user_groups'] = dict(sql(self.db, am.user_id == user.id, ag.id, ag.role, as_dict=False, join=ag.on(am.group_id == ag.id)))
        session['user_id'] = user.id
        session['user'] = user.as_dict()
        if self.app.realtime_endpoint:
            share_user(self.app)
        return session.sid, user.id

    def initialize_auth(self, settings):

        self.get_next = settings.get('get_next','')
        self.login_url = settings.get('login_url','')

        # updating settings and message attributes
        self.settings.update(settings)
        self.messages.update(settings.get('messages',{}))
        self.use_username = self.settings.use_username


    def logout(self):
        """
        Log out user and erase session
        :return: None
        """
#        for key in session.keys():
#            del session[key]
        session.clear()

    def id_group(self, group_name):
        """
        find id from group name
        :param group_name: string: auth group name
        :return: int: group id
        """
        ret = column(self.db,self.db.auth_group.role == group_name,'id')
        return ret and ret[0]

    @property
    def user_groups(self):
        return session['user_groups']

    @property
    def user(self):
        return session.get('user')

    @property
    def user_id(self):
        return session.get('user_id')


    @property
    def token(self):
        return session.sid

    def requires_login(self,func):
        def wrapper(*args,**kwargs):
            if 'user_id' in session:
                return func(*args,**kwargs)
            return redirect(self.app.url_prefix + '/' + self.login_url)
        wrapper.__name__ = func.__name__
        return wrapper

    @property
    def user(self):
        """
        Get user from db or cache user for future calls
        :return:
        """
        id = session['user_id']
        if not id in user_cache:
            user_cache[id] = self.db.auth_user[session['user_id']]
        return user_cache[id]

    def accessible_query(self, name, table, user_id=None):
        """
        Returns a query with all accessible records for user_id or
        the current logged in user
        this method does not work on GAE because uses JOIN and IN

        Example:
            Use as::

                db(auth.accessible_query('read', db.mytable)).select(db.mytable.ALL)

        """
        if not user_id:
            user_id = self.user_id
        db = self.db
        if isinstance(table, str) and table in self.db.tables():
            table = self.db[table]
        elif isinstance(table, (Set, Query)):
            # experimental: build a chained query for all tables
            if isinstance(table, Set):
                cquery = table.query
            else:
                cquery = table
            tablenames = db._adapter.tables(cquery)
            for tablename in tablenames:
                cquery &= self.accessible_query(name, tablename,
                                                user_id=user_id)
            return cquery
        if not isinstance(table, str) and \
                self.has_permission(name, table, 0, user_id):
            return table.id > 0
        membership = db.auth_membership
        permission = db.auth_permission
        query = table.id.belongs(
            db(membership.user_id == user_id)
            (membership.group_id == permission.group_id)
            (permission.name == name)
            (permission.table_name == table)
                ._select(permission.record_id))
        return query


    def has_permission(
            self,
            name='any',
            table_name='',
            record_id=0,
            user_id=None,
            group_id=None,
    ):
        """
        Checks if user_id or current logged in user is member of a group
        that has 'name' permission on 'table_name' and 'record_id'
        if group_id is passed, it checks whether the group has the permission
        """

        if not group_id and self.settings.everybody_group_id and \
                self.has_permission(
                    name, table_name, record_id, user_id=None,
                    group_id=self.settings.everybody_group_id):
            return True

        if not user_id and not group_id and self.user:
            user_id = self.user.id
        if user_id:
            membership = self.db.auth_membership
            rows = self.db(membership.user_id
                           == user_id).select(membership.group_id)
            groups = set([row.group_id for row in rows])
            if group_id and not group_id in groups:
                return False
        else:
            groups = set([group_id])
        permission = self.db.auth_permission
        rows = self.db(permission.name == name)(permission.table_name
                                                == str(table_name))(permission.record_id
                                                                    == record_id).select(permission.group_id)
        groups_required = set([row.group_id for row in rows])
        if record_id:
            rows = self.db(permission.name
                           == name)(permission.table_name
                                    == str(table_name))(permission.record_id
                                                        == 0).select(permission.group_id)
            groups_required = groups_required.union(set([row.group_id
                                                         for row in rows]))
        if groups.intersection(groups_required):
            r = True
        else:
            r = False
        return r

    def add_permission(
        self,
        group_id,
        name='any',
        table_name='',
        record_id=0,
        ):
        """
        Gives group_id 'name' access to 'table_name' and 'record_id'
        """

        permission = self.db.auth_permission
        record = self.db(permission.group_id == group_id)(permission.name == name)(permission.table_name == str(table_name))(
                permission.record_id == long(record_id)).select(limitby=(0,1), orderby_on_limitby=False).first()
        if record:
            id = record.id
        else:
            id = permission.insert(group_id=group_id, name=name,
                                   table_name=str(table_name),
                                   record_id=long(record_id))
        return id

    def del_permission(
        self,
        group_id,
        name='any',
        table_name='',
        record_id=0,
        ):
        """
        Revokes group_id 'name' access to 'table_name' and 'record_id'
        """

        permission = self.db.auth_permission
        return self.db(permission.group_id == group_id)(permission.name
                 == name)(permission.table_name
                           == str(table_name))(permission.record_id
                 == long(record_id)).delete()
