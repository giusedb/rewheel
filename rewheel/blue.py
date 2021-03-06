import sys, os
from itertools import ifilter, imap
from operator import itemgetter, attrgetter
from flask import Blueprint, request, Response, redirect, current_app, send_from_directory
from redis import Redis

from .import_hook import jdumps, jloads
from rewheel.push import realtime_commit
from .base import Message, TableResource, ResourceManager, ManyToManyRelation, session
from .utils import current, NestedDict, json_mime

from .exceptions import HTTP, ValidationError
from .authentication import Auth, UserResource
from .push import share_user
from time import time
from logging import getLogger
from traceback import format_tb
from flask_cors import CORS, cross_origin

last_build = time()
log = getLogger('rewheel')

class ReturnObject:
    """
    Empty class
    """
    text = ''

default_auth_permissions = dict(
    auth_user = dict(

    ),
    auth_group = dict(),
    auth_membership = dict(),
    auth_permissions = dict(),

)

class RewheelApplication(Blueprint):
    """
    Flask integrated Application exposing restful and real time resources
    """

    def __init__(self, name, import_name, static_folder=None,
                 static_url_path=None, template_folder=None,
                 url_prefix=None, subdomain=None, url_defaults=None,
                 root_path=None, config=None, auth_permissions=dict()):
        """
        Create reWheel application as a blueprint
        :param name:
        :param import_name:
        :param static_folder:
        :param static_url_path:
        :param template_folder:
        :param url_prefix:
        :param subdomain:
        :param url_defaults:
        :param auth_permissions:
        :param root_path:
        """
        self._config = config or {}
        self.debug = False
        self.name = name
        url_prefix = url_prefix or self.config.get('url_prefix') or ('/%s' % name)
        # collecting arguments for authenticator instance
        Blueprint.__init__(self,name,import_name, static_folder,static_url_path, template_folder, url_prefix,subdomain,url_defaults,root_path)

        self.auth = Auth(self)

        # initialization
        self.resource_manager = ResourceManager()
        self._create_db, self._define_db, self._register_resource = None, [], []
        self.requires_login = self.auth.requires_login
        self.auth_permissions = dict((k,dict((x,auth_permissions.get(k,{}).get(x, default_auth_permissions[k].get(x))) for x in set(v).union(auth_permissions.get(k,(),)))) for k,v  in default_auth_permissions.iteritems())


        @self.route(r'/')
        def main():
            """
            Useless
            :return:
            """
            # getting this blueprint application
            app = current_app.blueprints.get(request.blueprint)
            if app:
                # if this application has a file named index.html it will be served
                if app.static_folder and os.path.exists('%s%sindex.html' % (app._static_folder, os.sep)):
                    return send_from_directory(app.static_folder, 'index.html')
            return 'rewheel main'

        @self.route('/api/logout', methods=['POST'])
        @cross_origin()
        def logout():
            self.auth.logout()
            if request.cookies:
                return redirect(self.url_prefix + '/' + self.auth.login_url)
            else:
                return Response(jdumps(dict(result = 'Logged out')),200)

        # @self.route('/static/<path:path>')
        # def serve_static(path):
        #     filename = '%s%s%s' % (self.static_folder, os.sep, path)
        #     if self.static_folder and os.path.exists(filename) and os.path.isfile(filename):
        #         return send_from_directory(self.static_folder, path)
        #     else:
        #         return Response('File not found',404)

        @self.route('/api/login')
        def login_get():
            return Response(jdumps(dict(error = 'login have to be run on POST method')),401,content_type=json_mime)

        @self.route('/api/login',methods=['POST','OPTIONS'])
        @cross_origin(vary_header = True, allow_headers=['token','application'])
        def login():
            # checking all arguments
            if not all(imap(request.values.__contains__,('username','password'))):
                return Response(jdumps(dict(error= 'username or password not supplied. you must use this endpoint with username and password as data keys')),400,content_type=json_mime,mimetype=json_mime)

            # logging in
            token, user_id = self.auth.login(*itemgetter('username','password')(request.values))

            # if user is not logged in return appropriate HTTP code
            if not token:
                return Response(jdumps({'error' : 'Unknown user or wrong password'}),401, content_type=json_mime)

            # share_user
            share_user(self)

            # is user is accepted return token
            return self.connection_status(token=str(token), user_id = user_id)

        @self.route(r'/<resource_name>.<verb>/<path:args>',methods = ['GET','POST','OPTIONS'])
        @self.route(r'/<resource_name>.<verb>',methods = ['GET','POST','OPTIONS'])
        @self.route(r'/<resource_name>/<verb>.<path:args>', methods=['GET', 'POST', 'OPTIONS'])
        @cross_origin()
        @self.auth.requires_login
        def restful(resource_name, verb, args = ''):
            """
            reWheel main url parse HTTP request in order to find appropriate method and calling it with args parsed
            :param resource: string : resource name
            :param verb: string: verb name
            :return: HTTPResponse to the browser
            """
            resource = self.resource_manager.resource(resource_name)
            if resource:
                method = getattr(resource, verb,None)
                if method:
                    current.response = ReturnObject()
                    try:
                        args, kwargs = self.get_args(request)
                        method(*(args.split('/') if args else ()), **kwargs)
                        # db.commit()
                        self.db._adapter.close()
                        if self.realtime_endpoint:
                            realtime_commit(self)
                    except HTTP as e:
                        return Response(e.message,e.status)
                    except ValidationError as e:
                        return Response(str(e),513,content_type=json_mime)
                    except Exception as e:
                        log.error(str(e))
                        log.debug('\n'.join(format_tb(sys.exc_info()[2])))
                        if self.debug:
                            return Response(
                                jdumps(dict(
                                    exception = str('%s : %s' % (type(e).__name__, e)),
                                    traceback = format_tb(sys.exc_info()[2]))
                                ),
                                status=500,
                            )
                        else:
                            return Response('Internal server error', status=500)
                    ret = current.response.text
                    if type(ret) in (dict,list):
                        return Response(jdumps(ret),content_type='application/json')
                    return ret
                else:
                    return Response('{"error" : "Resource %s has no %s verb"}' % (resource_name, verb), 404, content_type=json_mime)
            return Response('{"error" : "Resource %s not found"}' % resource_name, 404,content_type=json_mime)

        @self.route(r'/api/status',methods = ['GET','POST'])
        @cross_origin()
        def api_status():
            # from random import randrange
            # from time import sleep
            # if randrange(0,2):
            #     sleep(1)
            return self.connection_status()

        @self.route(r'/share_user')
        @cross_origin()
        def user_share():
            share_user(self)
            return ''

        @self.route(r'/api/resources', methods= ['GET', 'POST'])
        @cross_origin()
        def get_resources():
            return jdumps(filter(lambda x : type(x) is str and not x.startswith('auth_'), self.resource_manager.get_resources()))

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self,conf):
        # taking all configuration
        if type(conf) is dict:
            conf = NestedDict(conf)

        if self.name in conf:
            config = conf[self.name]

    def register(self, app, options, first_registration=False):
        self.main_app = app
        if 'config' in options:
            config = NestedDict(options['config'])
        with app.app_context():
            self.initialize_wheel(**config)
        super(RewheelApplication,self).register(app,config.get(self.name,{}) ,first_registration)

    def connection_status(self, token=None, user_id=None):
        """
        return Response for connection status
        :param token: str: session token
        :param user_id: int user id
        :return: Response : jsoned connection status
        """
        if not user_id:
            user_id = self.auth.user_id
        if not token:
            token = self.auth.token
        return Response(jdumps(dict(
            token=token,
            timestamp=time(),
            last_build=last_build,
            user_id=user_id,
            apiEndPoint=self.url_prefix,
            templates =  '%s/templates/' % (self.static_url_path or '/static'),
            realtimeEndPoint= self.realtime_endpoint if user_id else None,
            application=self.name,
        )), mimetype=json_mime,content_type=json_mime)

    def get_args(self,request):
        args = request.get_json()
        if args.__class__ is dict:
            return None, args
        args = request.values.get('args')
        if args:
            return None, jloads(args)
        if not args:
            args = dict(request.values.iteritems())
        return None, args


    def define_db(self,func):
        """
        Decorator for function who creates tables on db
        :param func:
        :return:
        """
        self._define_db.append(func)


    def register_resources(self,func):
        """
        Decorator for rewheel resource registration
        :param func:
        :return:
        """
        self._register_resource.append(func)


    def create_db(self,func):
        self._create_db = func

    def initialize_wheel(self,**config):
        """
        link all and execute all decorated functions
        :return:
        """

        def mkperms(permission):
            """
            Make permission object from tuple
            :param permission:
            :return:
            """
            if permission:
                return permission[0](self, *permission[1:])

        # getting correct config
        config = NestedDict(config)
        config = config.get(self.name,config)

        # configuring redis connection
        self.realtime_endpoint = config.get('realtime_endpoint')
        self.realtime_queue = config.get('realtime_queue_name')
        if self.realtime_queue and self.realtime_endpoint:
            redis_args = config.get('redis',{})
            if type(redis_args) is NestedDict:
                redis_args = redis_args.main
            redis_args.setdefault('host', 'localhost')
            redis_args.setdefault('port', 6379)
            log.info('Starting app %s using realtime on %s:%s with queue %s' % (self.name,redis_args['host'], redis_args['port'], self.realtime_queue))
            self.red = Redis(**redis_args)
        self.debug = config.get('debug',False)

        self.auth.initialize_auth(config['auth'])

        if not self._create_db:
            raise AttributeError('no create db found for %s application' % self.name)
        #TODO: definire meglio gli errori da assegnare in caso di mancanze con tanto di tipi e di valori da restituire (usare ispect per verificare la correttezza delle funzioni passate)

        log.info('starting application %s.' % self.name)
        log.debug('creating db connections')
        self.db = db = current.db = self._create_db(self)

        log.debug('creating authentication tables')
        self.auth.define_tables(db)

        log.debug('creating auth resources')
        for table in itemgetter('auth_user','auth_group')(db):
            # create permission dict
            permissions = dict((k,mkperms(v)) for k,v in self.auth_permissions[table._tablename].iteritems())
            if table._tablename == 'auth_user':
                if not self.auth.use_username:
                    table.username.readable = False
                    table.username.writable = False
                self.resource_manager.register(UserResource(self,table,permissions=permissions))
                self.resource_manager.resource('auth_user').copy_email = not self.auth.use_username
            else:
                self.resource_manager.register(TableResource(self,table,permissions=permissions))

        log.debug('setting auth_membership as ManyToMany (auth_user,auth_group)')
        self.resource_manager.register_m2m( ManyToManyRelation (
            self.resource_manager.resource('auth_user'),
            self.resource_manager.resource('auth_group'),
            connection_table= db.auth_membership,
            fields=(db.auth_membership.user_id, db.auth_membership.group_id),
        ))

        log.debug('defining and migrating tables')
        for func in self._define_db:
            log.debug('defining tables from %s.' % func.__name__)
            func(self,db)

        log.debug('creating resources for %s.' % self.name)
        for func in self._register_resource:
            for resource in func(self,db):
                log.debug('registering resource %s for application %s.' % (resource.name, self.name))
                t = type(resource)
                if issubclass(t,TableResource):
                    self.resource_manager.register(resource)
                    if resource.private_args:
                        resource.private_args.auth = self.auth
                    if self.realtime_endpoint and self.realtime_queue:
                        resource.app = self
                        resource.enable_realtime()
                elif issubclass(t,ManyToManyRelation):
                    self.resource_manager.register_m2m(resource)
                    if self.realtime_endpoint and self.realtime_queue:
                        resource.app = self
                        resource.enable_realtime()