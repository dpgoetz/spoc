#!/usr/bin/env python

import sys
import eventlet
import time
import uuid
import random
from urlparse import urlparse
from eventlet.green import httplib
from eventlet.timeout import Timeout


class BadResponse(Exception):
    pass


class TimeoutResponse(Exception):
    def __init__(self, msg, path):
        Exception.__init__(self, msg)
        self.path = path


num_containers = 20
obj_size = 1 # keep low as i didn't write it to chunk the data or anything
pool_size = 50
timeout = 15 # low timeout to cause issue to happen more
put_wait = 20 # num seconds to wait after a timeout
too_many_failures = 5
cont_prefix = 'asdfasdfasdfasdf' # unique enough string for parsing logs

timeout_dict = {}
all_start = time.time()

if len(sys.argv) < 4:
    print sys.argv[0] + \
        ' https://storage_url/v1/acc_hash token num_obj [o_ring]'
    sys.exit()

num_objs = int(sys.argv[3])

oring = None

if len(sys.argv) == 5:
    obj_ring_path = sys.argv[4]
    from swift.common.ring import Ring
    oring = Ring(obj_ring_path)

failures = []

def do_stuff():
    storage_url = sys.argv[1]
    token = sys.argv[2]
    parsed_url = urlparse(storage_url)
    if parsed_url.scheme == 'http':
        connector = httplib.HTTPConnection
    else:
        connector = httplib.HTTPSConnection

    pool = eventlet.greenpool.GreenPool(pool_size)
    obj_queue = []

    def make_req(put_path, sleep_time=0, retry_queue=None, method='PUT'):
        if sleep_time:
            print 'sleeping for %s' % put_path
            eventlet.sleep(sleep_time)
        body = ''
        if method == 'PUT' and len(put_path.split('/')) == 5:
            body = '1' * obj_size
        try:
            with Timeout(timeout):
                conn = connector(parsed_url.netloc)
                conn.request(method, put_path, body=body,
                              headers={'X-Auth-Token': token})
                resp = conn.getresponse()

                if resp.status == 401:
                    print '401ed'
                    obj_queue = []
                    return

                if resp.status == 404 and method == 'DELETE':
                    return

                if resp.status // 100 != 2:
                    if body == '' or retry_queue is None:
                        print \
                            'failed on a container put, give up: %s: %s: %s' \
                            % (resp.status, put_path, resp.getheaders())
                        obj_queue = []
                        return

        except (Exception, Timeout):
            if body == '' or retry_queue is None:
                print 'timed out on non-obj %s thing: %s' % (method, put_path)
                return
            failures.append(put_path)
            insertion_point = int(len(retry_queue) * .20)
            retry_queue.insert(insertion_point, (put_path, put_wait))
            timeout_dict[put_path] = timeout_dict.get(put_path, 0) + 1
            if timeout_dict[put_path] > too_many_failures:
                print 'total fail %s: %s' % (method, put_path)

    conts = ['%s/%s%s' % (parsed_url.path, cont_prefix, uuid.uuid4().hex)
             for i in xrange(num_containers)]
    start = time.time()
    for cont in conts:
        pool.spawn_n(make_req, cont)
    pool.waitall()
    print 'PUT %s conts @ %.2f r/s' % (num_containers,
                                       num_containers / (time.time()-start))

    objs = [('%s/%s' % (random.choice(conts), uuid.uuid4().hex), 0)
            for i in xrange(num_objs)]
    for obj_tup in objs:
        obj_queue.append(obj_tup)

    start = time.time()
    already_printed = set()
    while True:
        while obj_queue:
            put_path, sleep_time = obj_queue.pop(0)
            time_elapsed = int(time.time() - start)
            if time_elapsed > 1 and time_elapsed % 10 == 0 and \
                    time_elapsed not in already_printed:
                print 'have %s %s objs @ %.2f r/s' % ('PUT',
                    num_objs - len(obj_queue),
                    (num_objs - len(obj_queue)) / (time.time() - start))
                already_printed.add(time_elapsed)
            pool.spawn_n(make_req, put_path, sleep_time, obj_queue)
        pool.waitall()

        if not obj_queue:
            break

    print 'PUT %s objs @ %.2f r/s' % (num_objs,
                                      num_objs / (time.time()-start))

    for obj_tup in objs:
        obj_queue.append(obj_tup)

    start = time.time()
    already_printed = set()
    while True:
        while obj_queue:
            put_path, sleep_time = obj_queue.pop(0)
            time_elapsed = int(time.time() - start)
            if time_elapsed > 1 and time_elapsed % 10 == 0:
                print 'have %s %s objs @ %.2f r/s' % ('DELETED',
                    num_objs - len(obj_queue),
                    (num_objs - len(obj_queue)) / (time.time() - start))
                already_printed.add(time_elapsed)
            pool.spawn_n(make_req, put_path, sleep_time, obj_queue, 'DELETE')
        pool.waitall()

        if not obj_queue:
            break

    print 'DELETE %s objs @ %.2f r/s' % (num_objs,
                                         num_objs / (time.time()-start))

    start = time.time()
    for cont in conts:
        pool.spawn_n(make_req, cont, 0, None, 'DELETE')
    pool.waitall()
    print 'DELETE %s conts @ %.2f r/s' % (num_containers,
                                          num_containers / (time.time()-start))

    print 'there are %s fails' % len(failures)
    if oring:
        bad_ips = {}
        for obj_path in failures:
            try:
                junk, vrs, acc, cont, obj = obj_path.split('/')
            except Exception:
                continue
            part, nodes = oring.get_nodes(acc, cont, obj)
            for node in nodes:
                bad_ips[node['ip']] = bad_ips.get(node['ip'], 0) + 1

        ips_in_order = bad_ips.keys()
        ips_in_order.sort(cmp=lambda l,r: cmp(bad_ips[l], bad_ips[r]))
        for ip in ips_in_order:
            print 'object_server: %s had %s timeouts' % (ip, bad_ips[ip])


if __name__ == '__main__':
    do_stuff()
