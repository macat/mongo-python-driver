import os
import threading
import time
import unittest

from nose.plugins.skip import SkipTest

from pymongo import pool
from test_connection import get_connection
from testutils import delay

host = os.environ.get("DB_IP", "localhost")
port = int(os.environ.get("DB_PORT", 27017))

def looplet(greenlets):
    """World's smallest event loop; run until all greenlets are done
    """
    while True:
        done = True

        for g in greenlets:
            if not g.dead:
                done = False
                g.switch()

        if done:
            return


class GeventTest(unittest.TestCase):
    def test_gevent(self):
        """
        Demonstrate a problem with pymongo 2.1's connection pool: it relies on
        threading.local to associate sockets with threads, so it doesn't support
        greenlets. Since greenlets incorrectly *share* a socket, gevent throws
        the assertion error, "This event is already used by another greenlet."

        https://bitbucket.org/denis/gevent/src/63e08a21e032/gevent/socket.py#cl-163

        To reproduce this error, I schedule greenlets to do operations in this
        order:

        gr0: start a slow find()
        gr1: start a fast find()
            (here's the error: gr1 tries to wait on the same socket as gr0)
        gr1: get results
        gr0: get results
        """

        NOT_STARTED = 0
        SUCCESS = 1
        SKIP = 2

        try:
            from multiprocessing import Value, Process
        except ImportError:
            raise SkipTest('No multiprocessing module')
        
        outcome = Value('i', NOT_STARTED)

        # Do test in separate process so patch_socket() doesn't affect all
        # subsequent unittests in a big test run
        def do_test():
            try:
                from gevent import Greenlet
                from gevent import monkey
            except ImportError:
                outcome.value = SKIP
                return

            monkey.patch_socket()

            cx = get_connection()
            db = cx.pymongo_test
            db.test.remove(safe=True)
            db.test.insert({'_id': 1})

            results = {
                'find_fast_result': None,
                'find_slow_result': None,
            }

            history = []

            def find_fast():
                history.append('find_fast start')

                # With the old connection._Pool, this would throw
                # AssertionError: "This event is already used by another
                # greenlet"
                results['find_fast_result'] = list(db.test.find())
                history.append('find_fast done')

            def find_slow():
                history.append('find_slow start')

                # Javascript function that pauses for half a second
                where = delay(0.5)
                results['find_slow_result'] = list(db.test.find(
                    {'$where': where}
                ))

                history.append('find_slow done')

            gr0, gr1 = Greenlet(find_slow), Greenlet(find_fast)
            gr0.start()
            gr1.start_later(.1)
            gr0.join()
            gr1.join()

            self.assertEqual([{'_id': 1}], results['find_slow_result'])

            # Fails, since find_fast doesn't complete
            self.assertEqual([{'_id': 1}], results['find_fast_result'])

            self.assertEqual([
                'find_slow start',
                'find_fast start',
                'find_fast done',
                'find_slow done',
                ], history)

            outcome.value = SUCCESS

        proc = Process(target=do_test)
        proc.start()
        proc.join()

        if outcome.value == SKIP:
            raise SkipTest('gevent not installed')

        self.assertEqual(
            SUCCESS,
            outcome.value,
            'test failed'
        )

    def test_threads(self):
        """
        Test the same sequence of calls as the gevent tests to ensure my test
        is ok.

        Run this before the gevent tests, since gevent.monkey.patch_socket()
        can't be undone.
        """
        cx = get_connection()
        db = cx.pymongo_test
        db.test.remove()
        db.test.insert({'_id': 1})

        results = {
            'find_fast_result': None,
            'find_slow_result': None,
        }

        history = []

        def find_fast():
            history.append('find_fast start')
            # AssertionError: This event is already used by another greenlet
            results['find_fast_result'] = list(db.test.find())
            history.append('find_fast done')

        def find_slow():
            history.append('find_slow start')

            # Javascript function that pauses for half a second
            where = """function() {
                        var d = new Date((new Date()).getTime() + 500);
                        while (d > (new Date())) { }; return true;
                    }
                    """

            results['find_slow_result'] = list(db.test.find({'$where': where}))
            history.append('find_slow done')

        t0, t1 = threading.Thread(target=find_slow), threading.Thread(target=find_fast)
        t0.start()
        time.sleep(.1)
        t1.start()
        t0.join()
        t1.join()

        self.assertEqual([{'_id': 1}], results['find_slow_result'])

        # Fails, since find_fast doesn't complete
        self.assertEqual([{'_id': 1}], results['find_fast_result'])

        self.assertEqual([
            'find_slow start',
            'find_fast start',
            'find_fast done',
            'find_slow done',
        ], history)

    def test_greenlet(self):
        try:
            import greenlet
        except ImportError:
            raise SkipTest('greenlet not installed')

        cx_pool = pool.Pool(
            pair=(host,port),
            max_size=10,
            net_timeout=1000,
            conn_timeout=1000,
            use_ssl=False
        )

        sock_ids = []

        def get_socket():
            sock_ids.append(id(cx_pool.get_socket()))

        looplet([
            greenlet.greenlet(get_socket),
            greenlet.greenlet(get_socket),
        ])

        self.assertEqual(2, len(sock_ids))
        self.assertEqual(sock_ids[0], sock_ids[1])

    def test_greenlet_request(self):
        try:
            import greenlet
        except ImportError:
            raise SkipTest('greenlet not installed')

        # If greenlet module is available, then we can import greenlet_pool
        from pymongo import greenlet_pool

        pool_args = dict(
            pair=(host,port),
            max_size=10,
            net_timeout=1000,
            conn_timeout=1000,
            use_ssl=False,
        )

        for pool_class, expect_success in [
            (pool.Pool, False),
            (greenlet_pool.GreenletPool, True),
        ]:
            cx_pool = pool_class(**pool_args)
            sock_ids = []
            main = greenlet.getcurrent()

            def get_socket_in_request():
                cx_pool.start_request()
                main.switch()
                sock_ids.append(id(cx_pool.get_socket()))
                cx_pool.end_request()

            looplet([
                greenlet.greenlet(get_socket_in_request),
                greenlet.greenlet(get_socket_in_request),
            ])

            self.assertEqual(2, len(sock_ids))
            if expect_success:
                self.assertNotEqual(
                    sock_ids[0], sock_ids[1],
                    "Expected two greenlets to get two different sockets"
                )
            else:
                self.assertEqual(
                    sock_ids[0], sock_ids[1],
                    "Expected two greenlets to get same socket"
                )

if __name__ == '__main__':
    unittest.main()
