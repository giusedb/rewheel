import sys
from itertools import ifilter, imap
from operator import itemgetter
from flask import Blueprint, request, Response, redirect
from redis import Redis

from .import_hook import jdumps, jloads
from rewheel.push import realtime_commit
from .base import Message, TableResource, ResourceManager, ManyToManyRelation, session
from .utils import current, NestedDict, json_mime

from .exceptions import HTTP, ValidationError
from .authentication import Auth
from .push import share_user
from time import time
from logging import getLogger
from traceback import format_tb
from flask_cors import CORS, cross_origin


log = getLogger('rewheel')

class ReturnObject:
    """
    Empty class
    """
    text = ''


class RewheelApplication(Blueprint):
    """
    Flask integrated Application exposing restful and real time resources
    """

    def __init__(self, name, import_name, static_folder=None,
                 static_url_path=None, template_folder=None,
                 url_prefix=None, subdomain=None, url_defaults=None,
                 root_path=None, config=None):
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

        @self.route(r'/')
        def main():
            """
            Useless
            :return:
            """
            return 'rewheel main'

        @self.route('/api/logout')
        @cross_origin()
        def logout():
            if request.cookies:
                self.auth.logout()
                return redirect(self.url_prefix + '/' + self.auth.login_url)
            else:
                if session.get('user_id'):
                    return Response(jdumps(dict(result = 'Logged out')),200)
                else:
                    return Response(jdumps(dict(error = 'You are not logged in')),400)


        @self.route('/api/login')
        def login_get():
            return Response(jdumps(dict(error = 'login have to be run on POST method')),401,content_type=json_mime)

        @self.route('/api/login',methods=['POST'])
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

            # is user is accepted return token
            return self.connection_status(token=str(token), user_id = user_id)

        @self.route(r'/<resource_name>/<verb>/<path:args>',methods = ['GET','POST','OPTIONS'])
        @self.route(r'/<resource_name>/<verb>',methods = ['GET','POST','OPTIONS'])
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
                        kwargs = self.get_args(request)
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
                                    exception = str(e),
                                    traceback = format_tb(sys.exc_info()[2]))
                                ),
                                status=500,
                            )
                        else:
                            return Response('Internal server error', status=500)
                    ret = current.response.text
                    if type(ret) is dict:
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
        with app.app_context():
            self.initialize_wheel(**options['config'])
        super(RewheelApplication,self).register(app,options,first_registration)

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
            user_id=user_id,
            apiEndPoint=self.url_prefix,
            templates =  '%s/templates/' % (self.static_url_path or '/static'),
            realtimeEndPoint=self.realtime_endpoint,
            application=self.name,
        )), mimetype=json_mime,content_type=json_mime)

    def get_args(self,request):
        args = dict(request.values.iteritems())
        if request.data:
            args.update(jloads(request.data))
        return args


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

        # getting correct config
        config = NestedDict(config)
        config = config.get(self.name,config)

        # configuring redis connection
        self.realtime_endpoint = config.get('realtime_endpoint')
        self.realtime_queue = config.get('realtime_queue_name')
        if self.realtime_queue and self.realtime_endpoint:
            redis_args = dict((k,v) for k,v in config.items() if config.get('redis'))
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