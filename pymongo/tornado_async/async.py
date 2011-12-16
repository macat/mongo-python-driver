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

"""Tornado asynchronous Python driver for MongoDB."""

import tornado.ioloop, tornado.iostream
import greenlet
import socket
import unittest
import pymongo
from pymongo.errors import ConnectionFailure

class AsyncConnection(pymongo.Connection):
    def __init__(self, *args, **kwargs):
#        self.greenlets = {}
        # TODO: some way to resume the right greenlet when we get data in
        # _receive_data_on_socket()
        self.greenlets = set()
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
        return greenlet.getcurrent() in self.greenlets

    def connect(self, host, port):
        print 'connect(), is_async =', self._is_async()
        if self._is_async():
            self.usage_count = 0 # TODO: asyncmongo does this; what's this for?
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)

                # TODO: what if this would block?
                s.connect((host, port))

                # Code borrowed from asyncmongo
                # Tornado's IOStream makes the socket non-blocking
                self.__stream = tornado.iostream.IOStream(s)
                self.__stream.set_close_callback(self._socket_close)
                self.__alive = True
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

    def _send_message(self, message, with_last_error=False):
        if self._is_async():
            self._async_send_message(message)
            # Resume calling greenlet (probably the main greenlet)
            return greenlet.getcurrent().parent.switch()
        else:
            return super(AsyncConnection, self)._send_message(message, with_last_error)

    def _receive_data_on_socket(self, length, sock, request_id):
        if self._is_async():
            def callback(data):
                # TODO: get the *right* greenlet!!
                list(self.greenlets)[0].switch(data, None)

            # TODO HACK: is this as good as it gets?
            if not self.__stream:
                self.__stream = tornado.iostream.IOStream(sock)
                self.__stream.set_close_callback(self._socket_close)
                self.__alive = True
            self.__stream.read_bytes(length, callback=callback)
            data, error = greenlet.getcurrent().parent.switch()
            if error:
                raise error
            return data
        else:
            return super(AsyncConnection, self)._receive_data_on_socket(length, sock, request_id)

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
        if operation_name not in ('remove', 'update', 'insert', 'save', ):#'find'):
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
                        self.connection.greenlets.add(
                            greenlet.getcurrent()
                        )

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

                    # The operation has completed & we're back on the main greenlet
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

            def create_cursor():
                self.database.connection.greenlets.add(
                    greenlet.getcurrent()
                )

                result, error = None, None
                try:
                    # TODO: get_more(), tailable cursors
                    cursor = super(AsyncCollection, self).find(*args, **kwargs)
                    result = list(cursor)
                except Exception, e:
                    error = e
                client_callback(result, error)

            gr = greenlet.greenlet(create_cursor)

            # Start running find() in the greenlet
            gr.switch()

    def __repr__(self):
        return 'Async' + super(AsyncCollection, self).__repr__()


class AsyncTest(unittest.TestCase):
    def setUp(self):
        self.sync_cx = pymongo.Connection()
        self.sync_db = self.sync_cx.test
        self.sync_coll = self.sync_db.test_collection
        self.sync_coll.remove()
        self.sync_coll.ensure_index([('_id', pymongo.ASCENDING)])
        self.sync_coll.insert([{'_id': i} for i in range(1000)], safe=True)

    def test_repr(self):
        cx = AsyncConnection()
        self.assert_(repr(cx).startswith('Async'))
        db = cx.test
        self.assert_(repr(db).startswith('Async'))
        coll = db.test
        self.assert_(repr(coll).startswith('Async'))

    def mongoFindLambda(self, fn):
        """
        @param fn:          A function that executes a find() and takes a
                            callback function.
        @return:            result, error
        """
        result = {}
        def callback(data, error):
#            print 'data =', data
            result['data'] = data
            result['error'] = error
            tornado.ioloop.IOLoop.instance().stop()

        fn(callback)
        tornado.ioloop.IOLoop.instance().start()
        return result['data'], result['error']

    def assertMongoFoundLambda(self, fn, expected):
        """
        Test an async find() command.
        @param fn:          A function that executes a find() and takes a
                            callback function.
        @param expected:    Expected list of values returned
        """
        result, error = self.mongoFindLambda(fn)
        self.assertEqual(None, error)
        self.assertEqual(expected, result)

    def assertMongoFound(self, query, fields, expected):
        """
        Test an async find() command.
        @param query:       Query, like {'i': {'$gt': 1}}
        @param fields:      Field selector, like {'_id':false}
        @param expected:    Expected list of values returned
        """
        self.assertMongoFoundLambda(
            lambda cb: AsyncConnection().test.test_collection.find(
                query,
                fields,
                callback=cb
            ),
            expected
        )

    def test_find(self):
        # You know what's weird? MongoDB's default first batch is weird. It's
        # 101 records or 4MB, whichever comes first.
        self.assertMongoFound({'_id': 1}, {}, [{'_id': 1}])

    def test_find_default_batch(self):
        fn = lambda cb: AsyncConnection().test.test_collection.find(
            callback=cb,
            sort=[('_id', pymongo.ASCENDING)]
        )

        # You know what's weird? MongoDB's default first batch is weird. It's
        # 101 records or 4MB, whichever comes first.
        # TODO: why does this return all 1000 results, not 102?!
        self.assertMongoFoundLambda(fn, [{'_id': i} for i in range(102)])

if __name__ == '__main__':
    unittest.main()
