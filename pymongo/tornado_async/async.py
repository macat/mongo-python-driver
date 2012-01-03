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

"""Tornado asynchronous Python driver for MongoDB."""

import socket
import sys # TODO: remove once print statements are gone

import tornado.ioloop, tornado.iostream
import greenlet

import pymongo
from pymongo.errors import ConnectionFailure, InvalidOperation

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
        print >> sys.stderr, 'socket closed', self

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
        # TODO: explain
        if self._is_async():
            # TODO HACK: is this as good as it gets?
            if not self.__stream:
                # IOStream() will make the socket non-blocking
                self.__stream = tornado.iostream.IOStream(sock)
                self.__stream.set_close_callback(self._socket_close)

            def callback(data):
                # read_bytes() has completed, switch to the child greenlet and
                # parse the data.
                self.greenlet.switch(data, None)

            self.__stream.read_bytes(length, callback=callback)

            # Resume the main greenlet and allow it to continue executing while
            # we wait for the async callback. When the callback is executed,
            # we'll switch to the child greenlet and execute the lines below.
            data, error = greenlet.getcurrent().parent.switch()
            if error:
                raise error
            return data
        else:
            # We're not executing asynchronously, do normal pymongo processing.
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
        # Get pymongo's synchronous method for this operation
        super_obj = super(AsyncCollection, self)
        sync_method = super_obj.__getattribute__(operation_name)

        if operation_name not in ('update', 'insert', 'save', 'remove'):
            return sync_method
        else:
            def method(*args, **kwargs):

                client_callback = kwargs.get('callback', None)
                if not client_callback:
                    # Synchronous call, normal pymongo processing
                    return sync_method(*args, **kwargs)
                else:
                    # Async call. Since we have a callback, we pass safe=True.
                    del kwargs['callback']
                    kwargs['safe'] = True

                    def call_method():
                        self.connection.greenlet = greenlet.getcurrent()

                        # We're on a child greenlet
                        assert self.connection.greenlet.parent

                        result, error = None, None
                        try:
                            result = sync_method(*args, **kwargs)
                        except Exception, e:
                            error = e
                        return result, error

                    gr = greenlet.greenlet(call_method)
#                    print 'starting greenlet for %s: %s' % (
#                        operation_name, gr
#                    )

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
        client_callback = kwargs.get('callback')
        if not client_callback:
            # Synchronous call, normal pymongo processing
            return super(AsyncCollection, self).find(*args, **kwargs)
        else:
            # Async call
            del kwargs['callback']

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

    def find_one(self, *args, **kwargs):
        if 'callback' not in kwargs:
            # Synchronous
            return super(AsyncCollection, self).find_one(*args, **kwargs)

        if 'limit' in kwargs:
            raise ArgumentError("'limit' argument not allowed for find_one")

        # TODO: must we copy kwargs?
        client_callback = kwargs['callback']
        del kwargs['callback']

        def find_one_callback(result, error):
            # Turn single-document list into a plain document
            assert result is None or len(result) == 1
            client_callback(result[0] if result else None, error)

        # TODO: python2.4-compatible?
        self.find(*args, limit=-1, callback=find_one_callback, **kwargs)
        return None

    def __repr__(self):
        return 'Async' + super(AsyncCollection, self).__repr__()


class AsyncCursor(object):
    def __init__(self, cursor):
        """
        @param cursor:  Synchronous pymongo cursor
        """
        self._sync_cursor = cursor
        self.started = False

    def get_batch(self):
        """
        Call this on a child greenlet. Returns (result, error).
        """
        assert greenlet.getcurrent().parent, "Should be on child greenlet"
        result, error = None, None
        try:
            self.started = True
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
        assert self.started

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

    def __getattr__(self, name):
        """
        Support the chaining operators on cursors like limit() and batch_size()
        """
        if name in (
            'add_option', 'remove_option', 'limit', 'batch_size', 'skip',
            'max_scan', 'sort', 'count', 'distinct', 'hint', 'where', 'explain'
        ):
            if self.started:
                # TODO: don't raise exception in __getattr__, raise it when the
                # function's actually *called*
                raise InvalidOperation(
                    # TODO: better explanation, must pass callback in final
                    # chaining operator
                    "Can't call \"%s\" on a cursor once it's started"
                )

            def op(*args, **kwargs):
                # Apply the chaining operator to the Cursor
                getattr(self._sync_cursor, name)(*args, **kwargs)

                # Return the AsyncCursor
                return self

            return op
        else:
            raise AttributeError(name)
