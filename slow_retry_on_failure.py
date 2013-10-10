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


num_containers = 50
num_objs = 10000
obj_size = 1 # keep low as i didn't write it to chunk the data or anything
pool_size = 50
timeout = 60 # low timeout to cause issue to happen more
put_wait = 30 # num seconds to wait after a timeout
too_many_failures = 5
cont_prefix = 'asdfasdfasdfasdf' # unique enough string for parsing logs

timeout_dict = {}

if len(sys.argv) < 3:
    print 'failure_rep https://storage_url/v1/acc_hash auth_token'
    sys.exit()


def do_stuff():
    storage_url = sys.argv[1]
    token = sys.argv[2]
    parsed_url = urlparse(storage_url)
    if parsed_url.scheme == 'http':
        connector = httplib.HTTPConnection
    else:
        connector = httplib.HTTPSConnection

    pool = eventlet.greenpool.GreenPool(pool_size)
    obj_queue = eventlet.Queue()

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

                if resp.status // 100 != 2:
                    raise BadResponse('%s: %s: %s' % (resp.status, put_path, resp.getheaders()))
        except Timeout:
            if body == '' or retry_queue is None:
                print 'timed out on non-obj thing: %s' % put_path
                raise

            retry_queue.put((put_path, put_wait))
            timeout_dict[put_path] = timeout_dict.get(put_path, 0) + 1
            if timeout_dict[put_path] > too_many_failures:
                raise Exception(
                    '%s failed %s times' % (put_path, timeout_dict[put_path]))

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
        obj_queue.put(obj_tup)

    start = time.time()
    while True:
        while not obj_queue.empty():
            put_path, sleep_time = obj_queue.get()
            pool.spawn_n(make_req, put_path, sleep_time, obj_queue)
        pool.waitall()

        if obj_queue.empty():
            break

    print 'PUT %s objs @ %.2f r/s' % (num_objs,
                                      num_objs / (time.time()-start))

    for obj_tup in objs:
        obj_queue.put(obj_tup)

    start = time.time()
    while True:
        while not obj_queue.empty():
            put_path, sleep_time = obj_queue.get()
            pool.spawn_n(make_req, put_path, sleep_time, obj_queue, 'DELETE')
        pool.waitall()

        if obj_queue.empty():
            break

    print 'DELETE %s objs @ %.2f r/s' % (num_objs,
                                         num_objs / (time.time()-start))

    start = time.time()
    for cont in conts:
        pool.spawn_n(make_req, cont, 0, None, 'DELETE')
    pool.waitall()
    print 'DELETE %s conts @ %.2f r/s' % (num_containers,
                                          num_containers / (time.time()-start))

if __name__ == '__main__':
    do_stuff()
