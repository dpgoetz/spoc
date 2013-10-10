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


num_containers = 10
num_objs = 500
obj_size = 1 # keep low as i didn't write it to chunk the data or anything
pool_size = 50
timeout = 10 # low timeout to cause issue to happen more
put_wait = 60 # num seconds to wait after a timeout
too_many_failures = 5

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

    def put_thing(put_path, sleep_time=0, retry_queue=None):
        print 'lalala: %s' % (put_path,)
        if sleep_time:
            print 'sleeping for %s' % put_path
            eventlet.sleep(sleep_time)
        body = ''
        if len(put_path.split('/')) == 5:
            body = '1' * obj_size
        try:
            with Timeout(timeout):
                conn = connector(parsed_url.netloc)
                conn.request('PUT', put_path, body=body,
                              headers={'X-Auth-Token': token})
                resp = conn.getresponse()

                if resp.status // 100 != 2:
                    raise BadResponse('%s: %s' % (resp.status, put_path))
        except Timeout:
            if body == '' or retry_queue is None:
                raise

            retry_queue.put((put_path, put_wait))
            timeout_dict[put_path] = timeout_dict.get(put_path, 0) + 1
            if timeout_dict[put_path] > too_many_failures:
                raise Exception(
                    '%s failed %s times' % (put_path, timeout_dict[put_path]))

    conts = ['%s/%s' % (parsed_url.path, uuid.uuid4().hex)
             for i in xrange(num_containers)]
    start = time.time()
    list(pool.imap(put_thing, conts))
    print 'PUT %s conts @ %.2f r/s' % (num_containers,
                                       num_containers / (time.time()-start))

    for i in xrange(num_objs):
        obj_queue.put(('%s/%s' % (random.choice(conts), uuid.uuid4().hex), 0))

    start = time.time()
    while True:
        while not obj_queue.empty():
            put_path, sleep_time = obj_queue.get()
            pool.spawn_n(put_thing, put_path, sleep_time, obj_queue)
        pool.waitall()

        if obj_queue.empty():
            break

    print 'PUT %s objs @ %.2f r/s' % (num_objs,
                                      num_objs / (time.time()-start))

if __name__ == '__main__':
    do_stuff()
