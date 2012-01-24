import unittest
import time
import threading

from nose.plugins.skip import SkipTest

from test_connection import get_connection
from testutils import delay


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

if __name__ == '__main__':
    unittest.main()
