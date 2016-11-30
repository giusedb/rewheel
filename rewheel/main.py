__author__ = 'nihil'

import sys
from cPickle import dumps, loads as ploads
from csv import reader
from functools import *
from itertools import *
from time import sleep
from traceback import format_tb
try:
    from ujson import dumps as jdumps
except ImportError:
    from json import dumps as jdumps

from .utils import *


def getSessionId():
    v = current.request.cookies.get('session_id_%s' % application)
    return v and v.value

def ask_user(text,title,choices=[],labels=[],open=False,type='text',multiple=False,mandatory=True, default=None):
    """
    suspend request execution waiting for an answer by the user
    :param text: str: extended text to ask to user
    :param title: str: question title
    :param choices: iterable: choice return value
    :param labels: iterable : choice description
    :param open: bool: user can write an open answer
    :param type: str: unused
    :param multiple: bool: User can choose more then one answers
    :param mandatory: bool: request cannot be stopped
    :param default: str: default choice
    :return: str: user answer
    """
    wizid = red.hincrby('wizard',application,1)
    w = dict(
        text = text,
        title = title,
        wizId = wizid,
        type = type,
        # default = default,
    )
    if labels:
        w['labels'] = labels
    if open:
        w['open'] = True
    else:
        w['choices'] = choices
    if multiple:
        w['multiple'] = True
    if mandatory:
        w['mandatory'] = True
    if open and default:
        w['default'] = default
    rt_command('send_single',jdumps(dict(
        WIZARD = w )))
    wkey = 'wizResult:%s' % application
    for x in xrange(1200):
        sleep(0.05)
        ret = red.hget(wkey,wizid)
        if ret != None:
            ret = ploads(ret)
            red.hdel(wkey,wizid)
            if ret == 'null':
                ret = None
            return ret
    raise HTTP(514,'Answer timeout')

def csv_import(table,file_name,exclude=['id'],fixed={},field_translators={}):
    from .base import TableResource, ValidationError
    table_name = table.name if isinstance(table,TableResource) else table._tablename
    logger.info('Importing from CSV %s with %s on %s.' %(fixed,file_name,table_name ))
    translate = lambda x : None if x == '<NULL>' else x
    ids = {}
    if isinstance(table,TableResource):
        insert = partial(table.put, _check_permissions=False)
    else:
        insert = table.insert
    with open(file_name) as f:
        csv = reader(f)
        headers = map(lambda x : x.split('.')[-1], csv.next())
        exclude_idx = map(headers.index,filter(headers.__contains__,exclude))
        good = tuple(x for x in xrange(len(headers)) if x not in exclude_idx)
        igx = itemgetter(*good)
        good_headers = igx(headers)
        transfields = tuple(filter(good_headers.__contains__,field_translators.keys()))
        logger.debug('headers : %s' % ','.join(good_headers))
        IDX = headers.index('id')
        for line in csv:
            dct = dict(izip(good_headers,imap(translate,igx(line))))
            for field in transfields:
                dct[field] = field_translators[field].get(dct[field])
            dct.update(fixed)
            logger.debug('inserting on %s -> %s' % (table_name,dct))
            try:
                ids[line[IDX]] = str(insert(**dct))
            except ValidationError as e:
                logger.error('Errore di validazione : %s' % e)
            except Exception as e:
                logger.error('Error inserting %s\n\t%s\n\t%s' % (dct,'\n'.join(format_tb(sys.exc_info()[2])),e))
    for k in ids:
        if ids[k] == 'None':
            ids[k] = k
    return ids

def read_csv(filename):
    """
    read a CSV file and returns all records
    :param filename: file name
    :return: list of dicts
    """
    translate = lambda x : None if x == '<NULL>' else x
    with open(filename) as f:
        csv = reader(f)
        headers = map(lambda x : x.split('.')[-1], csv.next())
        return tuple(dict(izip(headers,imap(translate,line))) for line in csv)


