# Copyright 2011 10gen, Inc.
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

"""Tornado asynchronous Python driver for MongoDB."""

import socket
import sys
import unittest

import tornado.ioloop, tornado.iostream
import greenlet

import pymongo
from pymongo.errors import ConnectionFailure

# Tornado testing tools
from pymongo.tornado_async import eventually, puritanical

class AsyncConnection(pymongo.Connection):
    def __init__(self, *args, **kwargs):
        self.greenlet = None
        self.__stream = None
        super(AsyncConnection, self).__init__(*args, **kwargs)

    def __getattr__(self, name):
        """Get a database by name.

        Raises :class:`~pymongo.errors.InvalidName` if an invalid
        database name is used.

        :Parameters:
          - `name`: the name of the database to get
        """
        return AsyncDatabase(self, name)

    def _is_async(self):
        return greenlet.getcurrent() == self.greenlet

    def connect(self, host, port):
        print >> sys.stderr, 'connect(), is_async =', self._is_async()
        if self._is_async():
            self.usage_count = 0 # TODO: asyncmongo does this; what's this for?
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)

                # TODO: what if this would block?
                sock.connect((host, port))

                # Code borrowed from asyncmongo
                # Tornado's IOStream makes the socket non-blocking
                self.__stream = tornado.iostream.IOStream(sock)
                self.__stream.set_close_callback(self._socket_close)
            except socket.error, error:
                raise ConnectionFailure(error)

            if self.__dbuser and self.__dbpass:
                self.__authenticate = True
        else:
            # Synchronous call: regular pymongo processing
            return super(AsyncConnection, self).connect(host, port)

    def _socket_close(self):
        """async cleanup after the socket is closed by the other end"""
        # TODO: something?
        print 'socket closed', self

#    def _send_message(self, message, with_last_error=False):
#        if self._is_async():
#            self._async_send_message(message)
#            # Resume calling greenlet (probably the main greenlet)
#            return greenlet.getcurrent().parent.switch()
#        else:
#            return super(AsyncConnection, self)._send_message(
#                message,
#                with_last_error
#            )

    def _receive_data_on_socket(self, length, sock, request_id):
        if self._is_async():
            def callback(data):
                self.greenlet.switch(data, None)

            # TODO HACK: is this as good as it gets?
            if not self.__stream:
                self.__stream = tornado.iostream.IOStream(sock)
                self.__stream.set_close_callback(self._socket_close)
            self.__stream.read_bytes(length, callback=callback)
            data, error = greenlet.getcurrent().parent.switch()
            if error:
                raise error
            return data
        else:
            return super(AsyncConnection, self)._receive_data_on_socket(
                length, sock, request_id
            )

    def __repr__(self):
        return 'Async' + super(AsyncConnection, self).__repr__()

class AsyncDatabase(pymongo.database.Database):
    def __getattr__(self, collection_name):
        """
        Return an async Collection instead of a pymongo Collection
        """
        return AsyncCollection(self, collection_name)

    def __repr__(self):
        return 'Async' + super(AsyncDatabase, self).__repr__()

class AsyncCollection(pymongo.collection.Collection):
    def __getattribute__(self, operation_name):
        """
        Override pymongo Collection's attributes to replace the basic CRUD
        operations with async alternatives.
        # TODO: Note why this is __getattribute__
        # TODO: Just override them explicitly?
        @param operation_name:  Like 'find', 'remove', 'update', ...
        @return:                A proxy method that will implement the operation
                                asynchronously if provided a callback
        """
        if operation_name not in ('remove', 'update', 'insert', 'save', ):
            return super(AsyncCollection, self).__getattribute__(operation_name)
        else:
            def method(*args, **kwargs):
                # Get pymongo's synchronous method for this operation
                sync_method = getattr(self, operation_name)

                # TODO: support non-kw callback arg
                client_callback = kwargs.get('callback', None)
                if not client_callback:
                    # Synchronous call, normal pymongo processing
                    return sync_method
                else:
                    # Async call
                    del kwargs['callback']

                    def call_method():
                        self.connection.greenlet = greenlet.getcurrent()

                        result, error = None, None
                        try:
                            result = sync_method(*args, **kwargs)
                        except Exception, e:
                            error = e
                        return result, error

                    gr = greenlet.greenlet(call_method)
                    print 'starting greenlet for %s: %s' % (
                        operation_name, gr
                    )

                    # Start running the operation
                    result, error = gr.switch()

                    # The operation has completed & we're back on the main
                    # greenlet
                    assert not greenlet.getcurrent().parent
                    client_callback(result, error)

            return method

    def find(self, *args, **kwargs):
        """
        Run an async find(), rather than returning a pymongo Cursor, if callback
        is provided.
        """
        # TODO: support non-kw callback arg
        client_callback = kwargs.get('callback', None)
        if not client_callback:
            # Synchronous call, normal pymongo processing
            return super(AsyncCollection, self).find(*args, **kwargs)
        else:
            # Async call
            del kwargs['callback']

            # Code borrowed from pymongo Collection.find()
            if not 'slave_okay' in kwargs:
                kwargs['slave_okay'] = self.slave_okay
            if not 'read_preference' in kwargs:
                kwargs['read_preference'] = self.read_preference

            cursor = super(AsyncCollection, self).find(*args, **kwargs)
            async_cursor = AsyncCursor(cursor)

            def get_first_batch():
                self.database.connection.greenlet = greenlet.getcurrent()
                result, error = async_cursor.get_batch()

                # We're still in the child greenlet here, so ensure the callback
                # is executed on main greenlet
                tornado.ioloop.IOLoop.instance().add_callback(
                    lambda: client_callback(result, error)
                )

            gr = greenlet.greenlet(get_first_batch)

            # Start running find() in the greenlet
            gr.switch()

            # When the greenlet has sent the query on the socket, it will switch
            # back to the main greenlet, here, and we return to the caller.
            return async_cursor

    def __repr__(self):
        return 'Async' + super(AsyncCollection, self).__repr__()


class AsyncCursor(object):
    def __init__(self, cursor):
        """
        @param cursor:  Synchronous pymongo cursor
        """
        self._sync_cursor = cursor

    def get_batch(self):
        """
        Call this on a child greenlet. Returns (result, error).
        """
        result, error = None, None
        try:
            assert greenlet.getcurrent().parent, "Should be on child greenlet"
            self._sync_cursor._refresh()

            # TODO: Make this accessible w/o underscore hack
            result = self._sync_cursor._Cursor__data
            self._sync_cursor._Cursor__data = []
        except Exception, e:
            error = e

        return result, error

    def get_more(self, callback):
        """
        Get next batch of data asynchronously.
        @param callback:    A function taking parameters (result, error)
        """
        def next_batch():
            # This is executed on child greenlet
            self._sync_cursor.collection.database.connection.greenlet = \
                greenlet.getcurrent()
            result, error = self.get_batch()

            # We're still in the child greenlet here, so ensure the callback is
            # executed on main greenlet
            tornado.ioloop.IOLoop.instance().add_callback(
                lambda: callback(result, error)
            )

        gr = greenlet.greenlet(next_batch)
        gr.switch()

        # When the greenlet has sent the query on the socket, it will switch
        # back to the main greenlet, here, and we return to the caller.
        return None

    @property
    def alive(self):
        """Does this cursor have the potential to return more data?"""
        return self._sync_cursor.alive

    def __repr__(self):
        return 'Async' + super(AsyncCursor, self).__repr__()


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
        self.sync_coll.insert([{'_id': i} for i in range(1000)], safe=True)

    def test_repr(self):
        cx = AsyncConnection()
        self.assert_(repr(cx).startswith('Async'))
        db = cx.test
        self.assert_(repr(db).startswith('Async'))
        coll = db.test
        self.assert_(repr(coll).startswith('Async'))
        cursor = coll.find(callback=lambda: None)
        self.assert_(repr(cursor).startswith('Async'))
        cursor = coll.find()
        self.assertFalse(repr(cursor).startswith('Async'))

    def test_find(self):
        results = []
        def callback(result, error):
            self.assert_(error is None)
            results.append(result)

        AsyncConnection().test.test_collection.find(
                {'_id': 1},
            callback=callback
        )

        self.assertEventuallyEqual(
            [{'_id': 1}],
            lambda: results and results[0]
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

        cursor = AsyncConnection().test.test_collection.find(
            sort=[('_id', pymongo.ASCENDING)],
            callback=callback
        )

        # You know what's weird? MongoDB's default first batch is weird. It's
        # 100 records or 4MB, whichever comes first.
        self.assertEventuallyEqual(
            [{'_id': i} for i in range(101)],
            lambda: results and results[0]
        )

        # Next back has remainder of 1000 docs
        self.assertEventuallyEqual(
            [{'_id': i} for i in range(101, 1000)],
            lambda: len(results) >= 2 and results[1]
        )

        tornado.ioloop.IOLoop.instance().start()

    def test_batch_size(self):
        results = []
        cursor = None

        def callback(result, error):
            self.assert_(error is None)
            results.append(result)
            if cursor.alive:
                cursor.get_more(callback=callback)

        cursor = AsyncConnection().test.test_collection.find(
            sort=[('_id', pymongo.ASCENDING)],
            callback=callback,
            batch_size=10
        )

        expected_results = [
            [{'_id': i} for i in range(start_batch, start_batch + 10)]
            for start_batch in range(0, 1000, 10)
        ]

        self.assertEventuallyEqual(
            expected_results,
            lambda: results
        )

        tornado.ioloop.IOLoop.instance().start()


# TODO: test SON manipulators

if __name__ == '__main__':
    unittest.main()
