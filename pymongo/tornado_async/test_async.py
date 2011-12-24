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

import unittest
import sys

import pymongo
from pymongo.tornado_async import async
import tornado.ioloop

# Tornado testing tools
from pymongo.tornado_async import eventually, puritanical


class AsyncTest(
    puritanical.PuritanicalTest,
    eventually.AssertEventuallyTest
):
    def setUp(self):
        super(AsyncTest, self).setUp()
        self.sync_cx = pymongo.Connection()
        self.sync_db = self.sync_cx.test
        self.sync_coll = self.sync_db.test_collection
        self.sync_coll.remove()
        self.sync_coll.ensure_index([('s', pymongo.ASCENDING)], unique=True)
        self.sync_coll.insert(
            [{'_id': i, 's': hex(i)} for i in range(200)],
            safe=True
        )

    def test_repr(self):
        cx = async.AsyncConnection()
        self.assert_(repr(cx).startswith('AsyncConnection'))
        db = cx.test
        self.assert_(repr(db).startswith('AsyncDatabase'))
        coll = db.test
        self.assert_(repr(coll).startswith('AsyncCollection'))

    def test_cursor(self):
        """
        Test that we get a regular Cursor if we don't pass a callback to find(),
        and we get an AsyncCursor if we do pass a callback.
        """
        coll = async.AsyncConnection().test.foo
        # We're not actually running the find(), so null callback is ok
        cursor = coll.find(callback=lambda: None)
        self.assert_(isinstance(cursor, async.AsyncCursor))
        cursor = coll.find()
        self.assertFalse(isinstance(cursor, async.AsyncCursor))

    def test_find(self):
        results = []
        def callback(result, error):
            self.assert_(error is None)
            results.append(result)

        async.AsyncConnection().test.test_collection.find(
            {'_id': 1},
            callback=callback
        )

        def foo(*args, **kwargs):
            return results and results[0]

        self.assertEventuallyEqual(
            [{'_id': 1, 's': hex(1)}],
            foo
        )

        tornado.ioloop.IOLoop.instance().start()

    def test_find_default_batch(self):
        results = []
        cursor = None

        def callback(result, error):
            self.assert_(error is None)
            results.append(result)
            if cursor.alive:
                cursor.get_more(callback=callback)

        cursor = async.AsyncConnection().test.test_collection.find(
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
            self.assert_(error is None)
            results.append(result)
            if cursor.alive:
                cursor.get_more(callback=callback)

        cursor = async.AsyncConnection().test.test_collection.find(
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

    def test_update(self):
        results = []

        def callback(result, error):
            self.assert_(error is None)
            results.append(result)

        async.AsyncConnection().test.test_collection.update(
            {'_id': 5},
            {'$set': {'foo': 'bar'}},
            callback=callback,
        )

        self.assertEventuallyEqual(1, lambda: len(results))
        self.assertEventuallyEqual(1, lambda: results[0]['ok'])
        self.assertEventuallyEqual(True, lambda: results[0]['updatedExisting'])
        self.assertEventuallyEqual(1, lambda: results[0]['n'])
        self.assertEventuallyEqual(None, lambda: results[0]['err'])

        tornado.ioloop.IOLoop.instance().start()

    def test_update_bad(self):
        """
        Violate a unique index, make sure we handle error well
        """
        results = []

        def callback(result, error):
            self.assert_(isinstance(error, pymongo.errors.DuplicateKeyError))
            self.assertEqual(None, result)
            results.append(result)

        async.AsyncConnection().test.test_collection.update(
            {'_id': 5},
            {'$set': {'s': hex(4)}}, # There's already a document with s: hex(4)
            callback=callback,
        )

        self.assertEventuallyEqual(1, lambda: len(results))

        tornado.ioloop.IOLoop.instance().start()


# TODO: test insert, save, remove
# TODO: test SON manipulators
# TODO: test that it's really async, use asyncmongo test for inspiration
# TODO: replicate asyncmongo's whole test suite
# TODO: apply pymongo's whole suite to async

if __name__ == '__main__':
    unittest.main()
