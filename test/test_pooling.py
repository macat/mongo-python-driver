# Copyright 2009-2010 10gen, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test built in connection-pooling."""

import os
import random
import sys
import threading
import time
import unittest
sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

from pymongo.connection import Connection
from pymongo.pool import Pool
from pymongo.errors import ConfigurationError
from test_connection import get_connection
from testutils import delay

N = 50
DB = "pymongo-pooling-tests"
host = os.environ.get("DB_IP", "localhost")
port = int(os.environ.get("DB_PORT", 27017))


def one(s):
    """Get one element of a set"""
    return iter(s).next()


class MongoThread(threading.Thread):

    def __init__(self, test_case):
        threading.Thread.__init__(self)
        self.connection = test_case.c
        self.db = self.connection[DB]
        self.ut = test_case
        self.passed = False

    def run(self):
        self.run_mongo_thread()

        # No exceptions thrown
        self.passed = True

    def run_mongo_thread(self):
        raise NotImplementedError()


class SaveAndFind(MongoThread):

    def run_mongo_thread(self):
        for _ in xrange(N):
            rand = random.randint(0, N)
            id = self.db.sf.save({"x": rand}, safe=True)
            self.ut.assertEqual(rand, self.db.sf.find_one(id)["x"])


class Unique(MongoThread):

    def run_mongo_thread(self):
        for _ in xrange(N):
            self.connection.start_request()
            self.db.unique.insert({})
            self.ut.assertEqual(None, self.db.error())
            self.connection.end_request()


class NonUnique(MongoThread):

    def run_mongo_thread(self):
        for _ in xrange(N):
            self.connection.start_request()
            self.db.unique.insert({"_id": "mike"})
            self.ut.assertNotEqual(None, self.db.error())
            self.connection.end_request()


class Disconnect(MongoThread):

    def run_mongo_thread(self):
        for _ in xrange(N):
            self.connection.disconnect()


class NoRequest(MongoThread):

    def run_mongo_thread(self):
        self.connection.start_request()
        errors = 0
        for _ in xrange(N):
            self.db.unique.insert({"_id": "mike"})
            if self.db.error() is None:
                errors += 1

        self.connection.end_request()
        self.ut.assertEqual(0, errors)


def run_cases(ut, cases):
    threads = []
    for case in cases:
        for i in range(10):
            thread = case(ut)
            thread.start()
            threads.append(thread)

    for t in threads:
        t.join()

    for t in threads:
        assert t.passed, "%s.run_mongo_thread() threw an exception" % repr(t)


class OneOp(threading.Thread):

    def __init__(self, connection):
        threading.Thread.__init__(self)
        self.c = connection
        self.passed = False

    def run(self):
        pool = self.c._Connection__pool
        assert len(pool.sockets) == 1
        sock = one(pool.sockets)

        self.c.start_request()

        # start_request() hasn't yet moved the socket from the general pool into
        # the request
        assert len(pool.sockets) == 1
        assert one(pool.sockets) == sock

        self.c.test.test.find_one()

        # find_one() causes the socket to be used in the request, so now it's
        # bound to this thread
        assert len(pool.sockets) == 0
        assert len(pool.requests) == 1
        assert pool.requests.values()[0] == sock
        self.c.end_request()

        # The socket is back in the pool
        assert len(pool.sockets) == 1
        assert one(pool.sockets) == sock

        self.passed = True


class CreateAndReleaseSocket(threading.Thread):

    def __init__(self, connection, start_request, end_request):
        threading.Thread.__init__(self)
        self.c = connection
        self.start_request = start_request
        self.end_request = end_request
        self.passed = False

    def run(self):
        # Do an operation that requires a socket for .25 seconds.
        # test_max_pool_size uses this to spin up lots of threads requiring
        # lots of simultaneous connections, to ensure that Pool obeys its
        # max_size configuration and closes extra sockets as they're returned.
        # We need a delay here to ensure that more than max_size sockets are
        # needed at once.
        where = delay(.25)
        if self.start_request:
            self.c.start_request()
        self.c.test.test.find_one({'$where': where})
        if self.end_request:
            self.c.end_request()
        self.passed = True

class TestPooling(unittest.TestCase):

    def setUp(self):
        self.c = get_connection()

        # reset the db
        self.c.drop_database(DB)
        self.c[DB].unique.insert({"_id": "mike"})
        self.c[DB].unique.find_one()

    def tearDown(self):
        self.c = None

    def test_max_pool_size_validation(self):
        self.assertRaises(
            ValueError, Connection, host=host, port=port, max_pool_size=-1
        )

        self.assertRaises(
            ConfigurationError, Connection, host=host, port=port,
            max_pool_size='foo'
        )

        c = Connection(host=host, port=port, max_pool_size=100)
        self.assertEqual(c.max_pool_size, 100)

    def test_no_disconnect(self):
        run_cases(self, [NoRequest, NonUnique, Unique, SaveAndFind])

    def test_simple_disconnect(self):
        # Connection just created, expect 1 free socket
        self.assertEqual(1, len(self.c._Connection__pool.sockets))
        self.assertEqual(0, len(self.c._Connection__pool.requests))

        self.c.start_request()
        cursor = self.c.test.stuff.find()

        # Cursor hasn't actually caused a request yet, so there's still 1 free
        # socket.
        self.assertEqual(1, len(self.c._Connection__pool.sockets))
        self.assertEqual(1, len(self.c._Connection__pool.requests))
        self.assertEqual(None, self.c._Connection__pool.requests.values()[0])

        # Actually make a request to server, triggering a socket to be
        # allocated to the request
        list(cursor)
        self.assertEqual(0, len(self.c._Connection__pool.sockets))
        self.assertEqual(1, len(self.c._Connection__pool.requests))

        # Pool returns to its original state
        self.c.end_request()
        self.assertEqual(1, len(self.c._Connection__pool.sockets))
        self.assertEqual(0, len(self.c._Connection__pool.requests))

        self.c.disconnect()
        self.assertEqual(0, len(self.c._Connection__pool.sockets))
        self.assertEqual(0, len(self.c._Connection__pool.requests))

    def test_disconnect(self):
        run_cases(self, [SaveAndFind, Disconnect, Unique])

    def test_independent_pools(self):
        p = Pool((host, port), 10, None, None, False)
        self.c.start_request()
        self.assertEqual(set(), p.sockets)
        self.c.end_request()
        self.assertEqual(set(), p.sockets)

    def test_dependent_pools(self):
        c = get_connection()
        self.assertEqual(1, len(c._Connection__pool.sockets))
        c.start_request()
        c.test.test.find_one()
        self.assertEqual(0, len(c._Connection__pool.sockets))
        c.end_request()
        self.assertEqual(1, len(c._Connection__pool.sockets))

        t = OneOp(c)
        t.start()
        t.join()
        self.assert_(t.passed, "OneOp.run() threw exception")

        self.assertEqual(1, len(c._Connection__pool.sockets))
        c.test.test.find_one()
        self.assertEqual(1, len(c._Connection__pool.sockets))

    def test_multiple_connections(self):
        a = get_connection()
        b = get_connection()
        self.assertEqual(1, len(a._Connection__pool.sockets))
        self.assertEqual(1, len(b._Connection__pool.sockets))

        a.start_request()
        a.test.test.find_one()
        self.assertEqual(0, len(a._Connection__pool.sockets))
        a.end_request()
        self.assertEqual(1, len(a._Connection__pool.sockets))
        self.assertEqual(1, len(b._Connection__pool.sockets))
        a_sock = one(a._Connection__pool.sockets)

        b.end_request()
        self.assertEqual(1, len(a._Connection__pool.sockets))
        self.assertEqual(1, len(b._Connection__pool.sockets))

        b.start_request()
        b.test.test.find_one()
        self.assertEqual(1, len(a._Connection__pool.sockets))
        self.assertEqual(0, len(b._Connection__pool.sockets))

        b.end_request()
        b_sock = one(b._Connection__pool.sockets)
        b.test.test.find_one()
        a.test.test.find_one()

        self.assertEqual(b_sock,
                         one(b._Connection__pool.get_socket((b.host, b.port))))
        self.assertEqual(a_sock,
                         one(a._Connection__pool.get_socket((a.host, a.port))))

    def test_pool_with_fork(self):
        if sys.platform == "win32":
            raise SkipTest()

        try:
            from multiprocessing import Process, Pipe
        except ImportError:
            raise SkipTest()

        a = get_connection()
        a.test.test.find_one()
        self.assertEqual(1, len(a._Connection__pool.sockets))
        a_sock = one(a._Connection__pool.sockets)

        def loop(pipe):
            c = get_connection()
            self.assertEqual(1, len(c._Connection__pool.sockets))
            c.test.test.find_one()
            self.assertEqual(1, len(c._Connection__pool.sockets))
            pipe.send(one(c._Connection__pool.sockets).getsockname())

        cp1, cc1 = Pipe()
        cp2, cc2 = Pipe()

        p1 = Process(target=loop, args=(cc1,))
        p2 = Process(target=loop, args=(cc2,))

        p1.start()
        p2.start()

        p1.join(1)
        p2.join(1)

        p1.terminate()
        p2.terminate()

        p1.join()
        p2.join()

        cc1.close()
        cc2.close()

        b_sock = cp1.recv()
        c_sock = cp2.recv()
        self.assert_(a_sock.getsockname() != b_sock)
        self.assert_(a_sock.getsockname() != c_sock)
        self.assert_(b_sock != c_sock)
        self.assertEqual(a_sock,
                         one(a._Connection__pool.get_socket((a.host, a.port))))

    def test_request(self):
        # Check that Pool gives two different sockets in two calls to
        # get_socket() -- doesn't automatically put us in a request any more
        cx_pool = Pool(
            pair=(host,port),
            max_size=10,
            net_timeout=1000,
            conn_timeout=1000,
            use_ssl=False
        )

        sock0, from_pool0 = cx_pool.get_socket()
        sock1, from_pool1 = cx_pool.get_socket()

        self.assertNotEqual(sock0, sock1)
        self.assertFalse(from_pool0)
        self.assertFalse(from_pool1)

        # Now in a request, we'll get the same socket both times
        cx_pool.start_request()

        sock2, from_pool2 = cx_pool.get_socket()
        sock3, from_pool3 = cx_pool.get_socket()

        # Pool didn't keep reference to sock0 or sock1; sock2 and 3 are new
        self.assertNotEqual(sock0, sock2)
        self.assertNotEqual(sock1, sock2)

        # We're in a request, so get_socket() returned same sock both times
        self.assertEqual(sock2, sock3)
        self.assertFalse(from_pool2)

        # Second call to get_socket() returned from_pool=True
        self.assert_(from_pool3)

        # Return the sock to pool
        cx_pool.end_request()

        sock4, from_pool4 = cx_pool.get_socket()
        sock5, from_pool5 = cx_pool.get_socket()

        # Not in a request any more, we get different sockets
        self.assertNotEqual(sock4, sock5)

        # end_request() returned sock2 to pool
        self.assert_(from_pool4)
        self.assertEqual(sock4, sock2)

        # But since there was only one sock in the pool after end_request(),
        # the final call to get_socket() made a new sock
        self.assertFalse(from_pool5)

    def _test_max_pool_size(self, start_request, end_request):
        c = get_connection(max_pool_size=4)

        threads = []
        for i in range(40):
            t = CreateAndReleaseSocket(c, start_request, end_request)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        for t in threads:
            self.assert_(t.passed)

        # There's a race condition, so be lenient
        nsock = len(c._Connection__pool.sockets)
        self.assert_(
            abs(4 - nsock) < 4,
            "Expected about 4 sockets in the pool, got %d" % nsock
        )

    def test_max_pool_size(self):
        self._test_max_pool_size(False, False)

    def test_max_pool_size_with_request(self):
        self._test_max_pool_size(True, True)

    def test_max_pool_size_with_leaked_request(self):
        # Call start_request() but not end_request() -- this will leak requests,
        # leaving open sockets that aren't included in the pool at all.
        # pool.sockets will be empty because sockets are never returned to it.
        # The sockets will be closed when all references to them are deleted,
        # which will happen after the test completes and deletes its reference
        # to a Connection instance.
        self.assertRaises(
            AssertionError,
            self._test_max_pool_size,
            start_request=True,
            end_request=False
        )

    def test_max_pool_size_with_end_request_only(self):
        # Call end_request() but not start_request()
        self._test_max_pool_size(False, True)

if __name__ == "__main__":
    unittest.main()
