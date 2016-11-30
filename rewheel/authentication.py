import os
from rewheel.push import share_user

from .validators import *
from .utils import Storage, sql, column
from uuid import uuid4
from flask import session, redirect
from pydal.objects import Field
from dalproxy import find_db
from logging import getLogger
from .utils import current

log = getLogger('rewheel.authentication')

AUTHS = {}

is_not_empty = IS_NOT_EMPTY()


class Auth(object):
    """
    reWheel Authentication object class (taken by web2py)
    """
    _default_messages = Storage(
        label_first_name = 'Name',
        label_last_name = 'Family name',
        label_email = 'e-mail',
        label_reset_password = 'Password reset',
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
                        IS_NOT_IN_DB(db, 'auth_user.email',error_message=self.messages.email_taken),
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
        userfield = 'username' if self.use_username else 'email'
        password = str(db.auth_user.password.validate(str(password))[0])

        # getting user if exixsts
        user = db.auth_user(password = password, **{userfield : username})
        if not user:
            log.info('user %s is unknown as %s' % (username, userfield))
            return None, None
        log.info('user %(first_name)s %(last_name)s is accepted' % user)
        session['user_groups'] = dict(sql(self.db, am.user_id == user.id, am.id, ag.role, as_dict=False, join=ag.on(am.group_id == ag.id)))
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
        for key in session.keys():
            del session[key]
        # session.delete()

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