import unittest
import time
import pymongo
import threading
import gevent
from gevent import Greenlet

# From https://gist.github.com/1369699/5b674d81a84f945373e6bc8a4425eccb58fd3ebf
import geventmongo

class GreenletTest(unittest.TestCase):
    def _test_greenlet(self):
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
        cx = pymongo.Connection()
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

    def test_1_greenlet_official_pool(self):
        """
        Test greenlets with pymongo's standard connection pool
        """
        from gevent import monkey; monkey.patch_socket()
        self._test_greenlet()

    def test_2_greenlet_contributed_pool(self):
        """
        Test greenlets with gevent-safe pool contributed by
        Antonin Amand @gwik <antonin.amand@gmail.com>
        """
        from gevent import monkey; monkey.patch_socket()
        official_pool = pymongo.connection._Pool
        geventmongo.patch() # Replaces _Pool with a gevent-safe pool
        try:
            self._test_greenlet()
        finally:
            pymongo.connection._Pool = official_pool

    def test_0_thread(self):
        """
        Test the same sequence of calls as the gevent tests to ensure my test
        is ok.

        Run this before the gevent tests, since gevent.monkey.patch_socket()
        can't be undone.
        """
        cx = pymongo.Connection()
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

if __name__ == '__main__':
    unittest.main()
