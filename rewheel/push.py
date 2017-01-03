import traceback
import sys
from itertools import imap, groupby
from flask import session
from .utils import current
from .utils import ig0,ig1
from .import_hook import pdumps

def rt_command(func, app, *args, **kwargs):
    app.red.publish(app.realtime_queue, pdumps((app.name, session.sid, func, args, kwargs)))

def share_user(app):
    return
    # auth = app.auth
    # app.red.hset('SES%s' % app.name, session.sid, pdumps([auth.user_id,map(int,auth.user_groups.keys())]))


def mk_realtime_buffer():
    if not hasattr(current, 'update_log'):
        current.update_log = []
    if not hasattr(current, 'rt_permissions'):
        current.rt_permissions = []

lazy_verbs = set(('deleted', 'results'))

def realtime_commit(app):
    requires_commit = False
#    if current.request.application == application:
    if True:

        # rt_permissions = getattr(current, 'rt_perms', False)
        # if rt_permissions:
        #     rt_command('update_permissions', rt_permissions)


        rt_buffer = getattr(current, 'update_log', False)
        if rt_buffer:

            del current.update_log

            # import time
            # time.sleep(1)
            try:
                # items to send to all groups
                to_all, to_groups = [], {}  # { groups tuple -> item list }
                # items for certain groups only
                updates, inserted, m2m = [], [], []
                # distribute all rows in 2 different lists (to all and to subgroups)
                for verb, resource, item in rt_buffer:
                    if verb == 'm2m':
                        m2m.append((resource, item))
                    elif verb in lazy_verbs:
                        to_all.append((resource.name, verb, item))
                    else:
                        (updates if 'update' == verb else inserted).append((resource, item))

                # considering updated instances only
                for resource, items in ((res, imap(ig1, group)) for res, group in groupby(sorted(updates), ig0)):
                    # someone will change minimal permission
                    changed_permissions = []

                    for old, new in items:
                        if resource.minimal_permission.changed(old, new):
                            changed_permissions.append((old, new))
                        # if not unchanged permission items will be treated as normal inserted
                        else:
                            inserted.append((resource, new))

                    if changed_permissions:
                        # indexing all items by its ID
                        idx_items = dict((x['id'], x) for x in zip(*changed_permissions)[1])
                        xor_send = {}
                        # for the ones who really changes minimal permissions
                        old, new = zip(*changed_permissions)
                        new_groups, old_groups = map(resource.minimal_permission.get_groups, (new, old))

                        for id, groups in old_groups.iteritems():
                            if new_groups.get(id) == groups:
                                for n in new:
                                    inserted.append((resource, n))
                            else:
                                k = (old_groups[id], new_groups[id])
                                if k not in xor_send:
                                    xor_send[k] = set()
                                xor_send[k].add(id)

                        # finally send to or
                        for (old_g, new_g), ids in xor_send.iteritems():
                            og = set(old_g)  # .difference(new_g)
                            ng = set(new_g)  # .difference(old_g)
                            # if og and ng:
                            rt_command('send_or', app,
                                       ng,
                                       og,
                                       [[resource.name, 'results', idx_items[id]] for id in ids],
                                       [[resource.name, 'deleted', id] for id in ids],
                                       )
                            # elif og:
                            #     rt_command('send_groups',[[resource.name,'deleted',id] for id in ids],og)
                            # else:
                            #     rt_command('send_groups',[[resource.name,'results',idx_items[id]] for id in ids],ng)

                            # rold = old_groups.difference(new_groups)

                # considering inserted instances only
                for resource, items in ((res, map(ig1, group)) for res, group in groupby(sorted(inserted), ig0)):
                    idx_items = dict((x['id'], x) for x in items)
                    groups = resource.minimal_permission.get_groups(items)
                    for id, g in groups.iteritems():
                        if g:
                            if not g in to_groups:
                                to_groups[g] = []
                            to_groups[g].append((resource.name, 'results', idx_items[id]))

                for groups, items in to_groups.iteritems():
                    rt_command('send_groups', app, items, groups)

                if to_all:
                    rt_command('send_groups', app, to_all)

                if m2m:
                    rt_command('send_groups', app, [['m2m', resource, item] for resource, item in m2m])

                # red.publish(realtime_channel, dumps([application, getSessionId(), current.update_log]))

                requires_commit = True
                # print rt_buffer
            except Exception as e:
                print 'Error %s while committing realtime' % e
                print traceback.print_tb(sys.exc_traceback)

        rt_permissions = getattr(current, 'rt_permissions', False)
        if rt_permissions:
            requires_commit = True
            for p in rt_permissions:
                rt_command(*p)

        rt_buffer = getattr(current, 'log_update', False)
        if rt_buffer:
            try:
                # sincyng_db if necessary
                # for resource, verb in current.log_update.iteritems():
                #     res = resource_manager.resource(resource)
                #     for field in res.traversing:
                #         rt_sync_table(res.table,field,map(itemgetter('id'),map(itemgetter(1), current.log_update[resource].get('update',()))))

                # rt_command('enq', current.log_update)
                requires_commit = True
                # print current.update_log
            except:
                print 'Error committing'
                print current.update_log

    # if commit_handlers:
    #     for handler in commit_handlers:
    #         handler()

    return requires_commit

