# Copyright 2011-2012 10gen, Inc.
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

"""Test the Tornado asynchronous Python driver for MongoDB."""

import sys
import time
import unittest

import pymongo
import pymongo.objectid
from pymongo.tornado_async import async
import tornado.ioloop

# Tornado testing tools
from pymongo.tornado_async import eventually, puritanical

def delay(ms):
    """
    Create a delaying $where clause. Note that you can only have one of these
    Javascript functions running on the server at a time, see SERVER-4258.
    @param ms:  Time to delay, in milliseconds
    @return:    A Javascript $where clause that delays for that time
    """
    return """
        function() {
            var d = new Date((new Date()).getTime() + %d);
            while (d > (new Date())) { };
            return true;
        }
    """ % ms


class AsyncTest(
    puritanical.PuritanicalTest,
    eventually.AssertEventuallyTest
):
    def setUp(self):
        super(AsyncTest, self).setUp()
        self.sync_cx = pymongo.Connection()
        self.sync_db = self.sync_cx.test
        self.sync_coll = self.sync_db.test_collection

        # Make some test data
        self.sync_coll.remove()
        self.sync_coll.ensure_index([('s', pymongo.ASCENDING)], unique=True)
        self.sync_coll.insert(
            [{'_id': i, 's': hex(i)} for i in range(200)],
            safe=True
        )

    def async_connection(self):
        cx = async.AsyncConnection()
        loop = tornado.ioloop.IOLoop.instance()

        def connected(connection, error):
            loop.stop() # So we can exit async_connection()
            if error:
                raise error

        cx.open(connected)
        loop.start()
        assert cx.connected, "Couldn't connect to MongoDB"
        return cx

    def tearDown(self):
        self.sync_coll.remove()
        super(AsyncTest, self).tearDown()

    def test_repr(self):
        cx = async.AsyncConnection()
        self.assert_(repr(cx).startswith('AsyncConnection'))
        db = cx.test
        self.assert_(repr(db).startswith('AsyncDatabase'))
        coll = db.test
        self.assert_(repr(coll).startswith('AsyncCollection'))

    def test_connection(self):
        cx = async.AsyncConnection()

        # Can't access databases before connecting
        self.assertRaises(
            pymongo.errors.InvalidOperation,
            lambda: cx.some_database_name
        )

        self.assertRaises(
            pymongo.errors.InvalidOperation,
            lambda: cx.some_database_name
        )

        called = {'callback': False}

        def callback(connection, error):
            called['callback'] = True
            if error:
                raise error

        self.assertEventuallyEqual(
            True,
            lambda: cx.connected
        )

        self.assertEventuallyEqual(
            True,
            lambda: called['callback']
        )

        cx.open(callback)

        tornado.ioloop.IOLoop.instance().start()

    def test_cursor(self):
        """
        Test that we get a regular Cursor if we don't pass a callback to find(),
        and we get an AsyncCursor if we do pass a callback.
        """
        cx = self.async_connection()
        coll = cx.test.foo
        # We're not actually running the find(), so null callback is ok
        cursor = coll.find(callback=lambda: None)
        self.assert_(isinstance(cursor, async.AsyncCursor))
        self.assert_(cursor.started_async, "Cursor should start immediately")
        cursor = coll.find()
        self.assertFalse(isinstance(cursor, async.AsyncCursor))

    def test_find(self):
        results = []

        # Although in most tests I'm using the synchronized
        # self.async_connection() for convienience, here I'll really test
        # passing a callback to open() that does a find(), just to make sure
        # that works in the Node.js-driver style.
        def connected(connection, error):
            if error:
                raise error

            connection.test.test_collection.find(
                {'_id': 1},
                callback=found
            )

        def found(result, error):
            if error:
                raise error
            results.append(result)

        self.assertEventuallyEqual(
            [{'_id': 1, 's': hex(1)}],
            lambda: results[0]
        )

        async.AsyncConnection().open(callback=connected)

        tornado.ioloop.IOLoop.instance().start()

    def test_find_default_batch(self):
        results = []
        cursor = None

        def callback(result, error):
            if error:
                raise error
            results.append(result)
            if cursor.alive:
                cursor.get_more(callback=callback)

        cursor = self.async_connection().test.test_collection.find(
            {},
            {'s': 0}, # Don't return the 's' field
            sort=[('_id', pymongo.ASCENDING)],
            callback=callback
        )

        # You know what's weird? MongoDB's default first batch is weird. It's
        # 101 records or 1MB, whichever comes first.
        self.assertEventuallyEqual(
            [{'_id': i} for i in range(101)],
            lambda: results and results[0]
        )

        # Next batch has remainder of 1000 docs
        self.assertEventuallyEqual(
            [{'_id': i} for i in range(101, 200)],
            lambda: len(results) >= 2 and results[1]
        )

        tornado.ioloop.IOLoop.instance().start()

    def test_batch_size(self):
        batch_size = 3
        limit = 15
        results = []
        cursor = None

        def callback(result, error):
            if error:
                raise error
            results.append(result)
            if cursor.alive:
                cursor.get_more(callback=callback)

        cursor = self.async_connection().test.test_collection.find(
            {},
            {'s': 0}, # Don't return the 's' field
            sort=[('_id', pymongo.ASCENDING)],
            callback=callback,
            batch_size=batch_size,
            limit=limit,
        )

        expected_results = [
            [{'_id': i} for i in range(start_batch, start_batch + batch_size)]
            for start_batch in range(0, limit, batch_size)
        ]

        self.assertEventuallyEqual(
            expected_results,
            lambda: results
        )

        tornado.ioloop.IOLoop.instance().start()

    def __disabled__test_chaining(self):
        """
        Test chaining operations. The idea is to support an API like:
            db.test.find().batch_size(5).limit(10, callback=my_callback)
        ... where callback must be an argument to the *last* chained operator,
        triggering the cursor to actually launch the operation. But at the
        moment I'm abandoning this idea and requiring you to pass in batch_size,
        limit, etc. as keyword arguments to find() along with the callback, and
        I'm saying you can't do chaining with async.
        """
        results = []
        def callback(result, error):
            if error:
                raise error
            self.assert_(error is None, str(error))
            results.append(result)

        test_collection = self.async_connection().test.test_collection

        for opname, params in [
            ('limit', (5, )),
            ('batch_size', (5, )),
            ('add_option', (5, )),
            ('remove_option', (5, )),
            ('skip', (5, )),
            ('max_scan', (5, )),
            ('sort', ('foo', )),
            ('sort', ('foo', 1)),
            ('sort', ([('foo', 1)], )),
            ('count', ()),
            ('distinct', ()),
            ('explain', ()),
            ('hint', ('index_name',)),
            ('where', ('where_clause', )),
        ]:
            cursor_with_callback = test_collection.find(
                {'_id': 1},
                callback=callback
            )

            # Can't call limit(), batch_size(), etc. on a cursor that already
            # has a callback set. Callback must be the last option set on a
            # cursor.
            self.assertRaises(
                pymongo.errors.InvalidOperation,
                lambda: getattr(cursor_with_callback, opname)(*params)
            )

        self.assertEventuallyEqual(
            [{'_id': 1, 's': hex(1)}],
            lambda: results[0]
        )

        tornado.ioloop.IOLoop.instance().start()

        results = []
        cursor = test_collection.find().limit(5).sort('foo', pymongo.ASCENDING)
        cursor.batch_size(5, callback=callback)

        expected_results = [
            [{'_id': i, 's': hex(i)}]
            for j in range(2) for i in range(5 * j, 5 * (j+1))
        ]

        self.assertEventuallyEqual(expected_results, lambda: results[0])

        tornado.ioloop.IOLoop.instance().start()

    def test_find_is_async(self):
        """
        Confirm find() is async by launching three operations which will finish
        out of order. Also test that AsyncConnection doesn't reuse sockets
        incorrectly.
        """
        # Make a big unindexed collection that will take a long time to query
        self.sync_db.drop_collection('big_coll')
        self.sync_db.big_coll.insert([
            {'s': hex(s)} for s in range(10000)
        ])

        cx = self.async_connection()

        results = []

        def callback(result, error):
            # print >> sys.stderr, 'result',result
            if error:
                raise error
            results.append(result)

        # Launch 3 find operations for _id's 1, 2, and 3, which will finish in
        # order 2, 3, then 1.
        loop = tornado.ioloop.IOLoop.instance()

        now = time.time()

        # This find() takes 1 second
        loop.add_timeout(
            now + 0.1,
            lambda: cx.test.test_collection.find(
                {'_id': 1, '$where': delay(1000)},
                fields={'s': True, '_id': False},
                callback=callback
            )
        )

        # Very fast lookup
        loop.add_timeout(
            now + 0.2,
            lambda: cx.test.test_collection.find(
                {'_id': 2},
                fields={'s': True, '_id': False},
                callback=callback
            )
        )

        # Find {'i': 3} in big_coll -- even though there's only one such record,
        # MongoDB will have to scan the whole table to know that. We expect this
        # to be faster than 1 second (the $where clause above) and slower than
        # the indexed lookup above.
        loop.add_timeout(
            now + 0.3,
            lambda: cx.test.big_coll.find(
                {'s': hex(3)},
                fields={'s': True, '_id': False},
                callback=callback
            )
        )

        # Results were appended in order 2, 3, 1
        self.assertEventuallyEqual(
            [[{'s': hex(s)}] for s in (2, 3, 1)],
            lambda: results,
            timeout_sec=1.3
        )

        tornado.ioloop.IOLoop.instance().start()

    def test_find_one(self):
        results = []
        def callback(result, error):
            if error:
                raise error
            results.append(result)

        self.async_connection().test.test_collection.find_one(
            {'_id': 1},
            callback=callback
        )

        self.assertEventuallyEqual(
            # Not a list of docs, find_one() returns the doc itself
            {'_id': 1, 's': hex(1)},
            lambda: results[0]
        )

        tornado.ioloop.IOLoop.instance().start()

    def test_find_one_is_async(self):
        """
        Confirm find_one() is async by launching two operations which will
        finish out of order.
        """
        results = []

        def callback(result, error):
            # print >> sys.stderr, 'result',result
            if error:
                raise error
            results.append(result)

        # Launch 2 find_one operations for _id's 1 and 2, which will finish in
        # order 2 then 1.
        loop = tornado.ioloop.IOLoop.instance()

        cx = self.async_connection()

        # This find_one() takes half a second
        loop.add_timeout(
            time.time() + 0.1,
            lambda: cx.test.test_collection.find_one(
                {'_id': 1, '$where': delay(500)},
                fields={'s': True, '_id': False},
                callback=callback
            )
        )

        # Very fast lookup
        loop.add_timeout(
            time.time() + 0.2,
            lambda: cx.test.test_collection.find_one(
                {'_id': 2},
                fields={'s': True, '_id': False},
                callback=callback
            )
        )

        # Results were appended in order 2, 1
        self.assertEventuallyEqual(
            [{'s': hex(s)} for s in (2, 1)],
            lambda: results
        )

        tornado.ioloop.IOLoop.instance().start()

    def test_update(self):
        results = []

        def callback(result, error):
            if error:
                raise error
            results.append(result)

        self.async_connection().test.test_collection.update(
            {'_id': 5},
            {'$set': {'foo': 'bar'}},
            callback=callback,
        )

        self.assertEventuallyEqual(1, lambda: results[0]['ok'])
        self.assertEventuallyEqual(True, lambda: results[0]['updatedExisting'])
        self.assertEventuallyEqual(1, lambda: results[0]['n'])
        self.assertEventuallyEqual(None, lambda: results[0]['err'])

        tornado.ioloop.IOLoop.instance().start()
        self.assertEqual(1, len(results))

    def test_update_bad(self):
        """
        Violate a unique index, make sure we handle error well
        """
        results = []

        def callback(result, error):
            self.assert_(isinstance(error, pymongo.errors.DuplicateKeyError))
            self.assertEqual(None, result)
            results.append(result)

        self.async_connection().test.test_collection.update(
            {'_id': 5},
            {'$set': {'s': hex(4)}}, # There's already a document with s: hex(4)
            callback=callback,
        )

        self.assertEventuallyEqual(None, lambda: results[0])
        tornado.ioloop.IOLoop.instance().start()
        self.assertEqual(1, len(results))

    def test_insert(self):
        results = []

        def callback(result, error):
            # print >> sys.stderr, 'result', result
            if error:
                raise error
            results.append(result)

        self.async_connection().test.test_collection.insert(
            {'_id': 201},
            callback=callback,
        )

        # insert() returns new _id
        self.assertEventuallyEqual(201, lambda: results[0])

        tornado.ioloop.IOLoop.instance().start()
        self.assertEqual(1, len(results))

    def test_insert_many(self):
        results = []

        def callback(result, error):
            # print >> sys.stderr, 'result', result
            if error:
                raise error
            results.append(result)

        self.async_connection().test.test_collection.insert(
            [{'_id': i, 's': hex(i)} for i in range(201, 211)],
            callback=callback,
        )

        self.assertEventuallyEqual(range(201, 211), lambda: results[0])

        tornado.ioloop.IOLoop.instance().start()
        self.assertEqual(1, len(results))

    def test_insert_bad(self):
        """
        Violate a unique index, make sure we handle error well
        """
        results = []

        def callback(result, error):
            # print >> sys.stderr, 'result', result
            self.assert_(isinstance(error, pymongo.errors.DuplicateKeyError))
            self.assertEqual(None, result)
            results.append(result)

        self.async_connection().test.test_collection.insert(
            {'s': hex(4)}, # There's already a document with s: hex(4)
            callback=callback,
        )

        self.assertEventuallyEqual(None, lambda: results[0])
        tornado.ioloop.IOLoop.instance().start()
        self.assertEqual(1, len(results))

    def test_insert_many_one_bad(self):
        """
        Violate a unique index in one of many updates, make sure we handle error
        well
        """
        results = []

        def callback(result, error):
            # print >> sys.stderr, 'result', result
            self.assert_(isinstance(error, pymongo.errors.DuplicateKeyError))
            results.append(result)

        self.async_connection().test.test_collection.insert(
            [
                {'_id': 201, 's': hex(201)},
                {'_id': 202, 's': hex(4)}, # Already exists
                {'_id': 203, 's': hex(203)},
            ],
            callback=callback,
        )


        # In async, even though first insert succeeded, result is None
        self.assertEventuallyEqual(None, lambda: results[0])

        tornado.ioloop.IOLoop.instance().start()

        self.assertEqual(1, len(results))

        # First insert should've succeeded
        self.assertEqual(
            [{'_id': 201, 's': hex(201)}],
            list(self.sync_db.test_collection.find({'_id': 201}))
        )

        # Final insert didn't execute, since second failed
        self.assertEqual(
            [],
            list(self.sync_db.test_collection.find({'_id': 203}))
        )

    def test_save_with_id(self):
        results = []

        def callback(result, error):
            # print >> sys.stderr, 'result', result
            if error:
                raise error
            results.append(result)

        self.async_connection().test.test_collection.save(
            {'_id': 5},
            callback=callback,
        )

        # save() returns the _id, in this case 5
        self.assertEventuallyEqual(5, lambda: results[0])

        tornado.ioloop.IOLoop.instance().start()
        self.assertEqual(1, len(results))

    def test_save_without_id(self):
        results = []

        def callback(result, error):
            # print >> sys.stderr, 'result', result
            if error:
                raise error
            results.append(result)

        self.async_connection().test.test_collection.save(
            {'fiddle': 'faddle'},
            callback=callback,
        )

        # save() returns the new _id
        self.assertEventuallyEqual(
            True,
            lambda: isinstance(results[0], pymongo.objectid.ObjectId)
        )

        tornado.ioloop.IOLoop.instance().start()
        self.assertEqual(1, len(results))

    def test_save_bad(self):
        """
        Violate a unique index, make sure we handle error well
        """
        results = []

        def callback(result, error):
            self.assert_(isinstance(error, pymongo.errors.DuplicateKeyError))
            self.assertEqual(None, result)
            results.append(result)

        self.async_connection().test.test_collection.save(
            {'_id': 5, 's': hex(4)}, # There's already a document with s: hex(4)
            callback=callback,
        )

        self.assertEventuallyEqual(None, lambda: results[0])

        tornado.ioloop.IOLoop.instance().start()
        self.assertEqual(1, len(results))

    def test_save_multiple(self):
        """
        TODO: what are we testing here really?
        """
        results = []

        def callback(result, error):
            # print >> sys.stderr, 'result',result
            if error:
                raise error
            results.append(result)

        cx = self.async_connection()

        for i in range(10):
            cx.test.test_collection.save(
                {'_id': i + 500, 's': hex(i + 500)},
                callback=callback
            )

        # Once all saves complete, results will be a list of _id's, from 500 to
        # 509, but not necessarily in that order since we're async.
        self.assertEventuallyEqual(
            range(500, 510),
            lambda: sorted(results)
        )

        tornado.ioloop.IOLoop.instance().start()
        self.assertEqual(10, len(results))

    def test_remove(self):
        """
        Remove a document twice, check that we get a success response first time
        and an error the second time.
        """
        results = []
        def callback(result, error):
            if error:
                raise error
            results.append(result)

        cx = self.async_connection()
        cx.test.test_collection.remove(
            {'_id': 1},
            callback=callback
        )

        tornado.ioloop.IOLoop.instance().add_timeout(
            0.1,
            lambda: cx.test.test_collection.remove(
                {'_id': 1},
                callback=callback
            )
        )

        # First time we remove, n = 1
        self.assertEventuallyEqual(1, lambda: results[0]['n'])
        self.assertEventuallyEqual(1, lambda: results[0]['ok'])
        self.assertEventuallyEqual(None, lambda: results[0]['err'])

        # Second time, document is already gone, n = 0
        self.assertEventuallyEqual(0, lambda: results[1]['n'])
        self.assertEventuallyEqual(1, lambda: results[1]['ok'])
        self.assertEventuallyEqual(None, lambda: results[1]['err'])

        tornado.ioloop.IOLoop.instance().start()
        self.assertEqual(2, len(results))

    def test_unsafe_insert(self):
        """
        Test that unsafe inserts with no callback still work
        """
        self.async_connection().test.test_collection.insert({'_id': 201})

        self.assertEventuallyEqual(
            1,
            lambda: len(list(self.sync_db.test_collection.find({'_id': 201})))
        )

        tornado.ioloop.IOLoop.instance().start()

    def test_unsafe_save(self):
        """
        Test that unsafe saves with no callback still work
        """
        self.async_connection().test.test_collection.save({'_id': 201})

        self.assertEventuallyEqual(
            1,
            lambda: len(list(self.sync_db.test_collection.find({'_id': 201})))
        )

        tornado.ioloop.IOLoop.instance().start()

    def test_command(self):
        assert False, "TODO"
        self.admin.command("ismaster", callback=callback)


# TODO: test that save and insert are async somehow? MongoDB's database-wide
#     write lock makes this hard. Maybe with two mongod instances.
# TODO: test master/slave, shard, replica set
# TODO: replicate asyncmongo's whole test suite?
# TODO: apply pymongo's whole suite to async
# TODO: don't use the 'test' database, use something that will play nice w/
#     Jenkins environment
# TODO: test SSL, I don't think my call to ssl.wrap_socket() in AsyncSocket is
#     right
# TODO: check that sockets are returned to pool, or closed, or something
# TODO: check that all cursors are closed!
# TODO: test unsafe remove

if __name__ == '__main__':
#    import greenlet
#    print >> sys.stderr, "main greenlet:", greenlet.getcurrent()
    unittest.main()
