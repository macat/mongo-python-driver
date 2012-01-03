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
import time

import pymongo
from pymongo.connection import _Pool
from pymongo.errors import ConnectionFailure, InvalidOperation


# TODO: sphinx-formatted docstrings

class AsyncSocket(object):
    """
    Replace socket with a class that yields from the current greenlet, if we're
    on a child greenlet, when making blocking calls, and uses Tornado IOLoop to
    wait for socket events to resume child greenlet.

    We only implement those socket methods actually used by pymongo: connect,
    sendall, and recv.
    """
    def __init__(self, *args, **kwargs):
        self.socket = socket.socket(*args, **kwargs)
        self._stream = None

    # Proxy some socket methods
    def setsockopt(self, *args, **kwargs):
        self.socket.setsockopt(*args, **kwargs)

    def settimeout(self, timeout):
        """
        Do nothing -- IOStream calls socket.setblocking(False), which does
        settimeout(0.0). We must not allow pymongo to set timeout to some other
        value (a positive number or None) or the socket will start blocking
        again.
        """
        pass

    @property
    def stream(self):
        """A Tornado IOStream that wraps the actual socket"""
        if not self._stream:
            # Tornado's IOStream sets the socket to be non-blocking
            self._stream = tornado.iostream.IOStream(self.socket)
        return self._stream

    def connect(self, address):
        """
        @param address: A tuple, (host, port)
        """
        child_gr = greenlet.getcurrent()
        assert child_gr.parent, "Should be on child greenlet"

        # This is run by IOLoop on the main greenlet when socket has connected;
        # switch back to child to continue processing
        def connect_callback():
            child_gr.switch()

        self.stream.connect(address, callback=connect_callback)

        # Resume main greenlet
        child_gr.parent.switch()

    def sendall(self, data):
        child_gr = greenlet.getcurrent()
        assert child_gr.parent, "Should be on child greenlet"

        # This is run by IOLoop on the main greenlet when data has been sent;
        # switch back to child to continue processing
        def sendall_callback():
            child_gr.switch()

        self.stream.write(data, callback=sendall_callback)

        # Resume main greenlet
        child_gr.parent.switch()

    def recv(self, num_bytes):
        """
        @param num_bytes:   Number of bytes to read from socket
        @return:            Data received
        """
        child_gr = greenlet.getcurrent()
        assert child_gr.parent, "Should be on child greenlet"

        # This is run by IOLoop on the main greenlet when socket has connected;
        # switch back to child to continue processing
        def recv_callback(data):
            child_gr.switch(data)

#        print >> sys.stderr, "starting read_bytes(%d) at %d" % (
#            num_bytes, time.time()
#        )
        self.stream.read_bytes(num_bytes, callback=recv_callback)

        # Resume main greenlet, returning the data received
#        print >> sys.stderr, "recv switching to parent: %s at %d" % (
#            child_gr.parent, time.time()
#        )
        return child_gr.parent.switch()
#
#class AsyncPool(pymongo.connection._Pool):
#    def get_socket(self, host, port):
#        sock = super(AsyncPool, self).get_socket(host, port)
#        return AsyncSocket(sock)

class AsyncConnection(pymongo.Connection):
    def __init__(self, *args, **kwargs):
        super(AsyncConnection, self).__init__(*args, **kwargs)
        # Replace the standard _Pool with an AsyncSocket-producing pool
        # TODO: Don't require overriding private attribute
        self._Connection__pool = _Pool(
            self._Connection__max_pool_size,
            self._Connection__net_timeout,
            self._Connection__conn_timeout,
            self._Connection__use_ssl,
            socket_factory=AsyncSocket
        )

    def __getattr__(self, name):
        """Get a database by name.

        Raises :class:`~pymongo.errors.InvalidName` if an invalid
        database name is used.

        :Parameters:
          - `name`: the name of the database to get
        """
        return AsyncDatabase(self, name)

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

        if operation_name not in ('update', 'insert', 'remove'):
            return sync_method
        else:
            def method(*args, **kwargs):
                client_callback = kwargs.get('callback')
                if client_callback and not callable(client_callback):
                    raise TypeError("callback must be callable")

                if 'callback' in kwargs:
                    kwargs = kwargs.copy()
                    del kwargs['callback']
                    kwargs['safe'] = True

                def call_method():
                    assert greenlet.getcurrent().parent, (
                        "Should be on child greenlet"
                    )

                    result, error = None, None
                    try:
                        result = sync_method(*args, **kwargs)
                    except Exception, e:
                        error = e

                    # Schedule the callback to be run on the main greenlet
                    if client_callback:
                        tornado.ioloop.IOLoop.instance().add_callback(
                            lambda: client_callback(result, error)
                        )

                child_gr = greenlet.greenlet(call_method)

                # Start running the operation
                child_gr.switch()

            return method

    def save(self, to_save, manipulate=True, safe=False, **kwargs):
        """Save a document in this collection."""
        if not isinstance(to_save, dict):
            raise TypeError("cannot save object of type %s" % type(to_save))

        if "_id" not in to_save:
            # This could be a synchronous or async operation, depending on
            # whether a callback is in kwargs.
            return self.insert(to_save, manipulate, safe=safe, **kwargs)
        else:
            client_callback = kwargs.get('callback')
            if client_callback:
                if not callable(client_callback):
                    raise TypeError("callback must be callable")

                kwargs = kwargs.copy()
                del kwargs['callback']

                # update() calls the callback with server's response to
                # getLastError, but we want to call it with the _id of the
                # saved document.
                def callback(result, error):
                    client_callback(
                        None if error else to_save['_id'],
                        error
                    )
            else:
                callback = None

            self.update({"_id": to_save["_id"]}, to_save, True,
                manipulate, _check_keys=True, safe=safe, callback=callback,
                **kwargs)

    def find(self, *args, **kwargs):
        """
        If passed a callback, run an async find(), and return an AsyncCursor,
        rather than returning a pymongo Cursor for synchronous operations.
        """
        client_callback = kwargs.get('callback')
        if not client_callback:
            # Synchronous operation
            return super(AsyncCollection, self).find(*args, **kwargs)
        else:
            if not callable(client_callback):
                raise TypeError("callback must be callable")

            kwargs = kwargs.copy()
            del kwargs['callback']

            cursor = super(AsyncCollection, self).find(*args, **kwargs)
            async_cursor = AsyncCursor(cursor)

            def get_first_batch():
                result, error = async_cursor.get_batch()
                tornado.ioloop.IOLoop.instance().add_callback(
                    lambda: client_callback(result, error)
                )

            # Start running find() in the greenlet
            gr = greenlet.greenlet(get_first_batch)
            gr.switch()

            # When the greenlet has sent the query on the socket, it will switch
            # back to the main greenlet, here, and we return to the caller.
            return async_cursor

    def find_one(self, *args, **kwargs):
        client_callback = kwargs.get('callback')
        if not client_callback:
            # Synchronous operation
            return super(AsyncCollection, self).find_one(*args, **kwargs)
        else:
            if not callable(client_callback):
                raise TypeError("callback must be callable")
            # We're going to pass limit of -1 to tell the server we're doing a
            # findOne, so ensure limit isn't already in kwargs.
            if 'limit' in kwargs:
                raise TypeError("'limit' argument not allowed for find_one")

            kwargs = kwargs.copy()
            del kwargs['callback']

            def find_one_callback(result, error):
                # Turn single-document list into a plain document
                assert result is None or len(result) == 1, (
                    "Got %d results from a findOne" % len(result)
                )

                client_callback(result[0] if result else None, error)

            # TODO: python2.4-compatible?
            self.find(*args, limit=-1, callback=find_one_callback, **kwargs)

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
        assert self.started, "get_more() called on cursor before it has started"

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
            def op(*args, **kwargs):
                if self.started:
                    raise InvalidOperation(
                        # TODO: better explanation, must pass callback in final
                        # chaining operator
                        "Can't call \"%s\" on a cursor once it's started"
                    )

                client_callback = kwargs.get('callback')
                if client_callback:
                    if not callable(client_callback):
                        raise TypeError("callback must be callable")

                    kwargs = kwargs.copy()
                    del kwargs['callback']

                # Apply the chaining operator to the Cursor
                getattr(self._sync_cursor, name)(*args, **kwargs)

                # Return the AsyncCursor
                return self

            return op
        else:
            raise AttributeError(name)
