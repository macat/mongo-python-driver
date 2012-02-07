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

have_ssl = True
try:
    import ssl
except ImportError:
    have_ssl = False

import tornado.ioloop, tornado.iostream
import greenlet
import os
import sys

from bson.binary import UUID_SUBTYPE
from bson.son import SON

import pymongo
from pymongo.errors import ConnectionFailure, InvalidOperation
from pymongo import common, helpers

__all__ = ['AsyncConnection']

# TODO: sphinx-formatted docstrings

def check_callable(kallable, required=False):
    if required and not kallable:
        raise TypeError("callable is required")
    if kallable is not None and not callable(kallable):
        raise TypeError("callback must be callable")

class TornadoSocket(object):
    """
    Replace socket with a class that yields from the current greenlet, if we're
    on a child greenlet, when making blocking calls, and uses Tornado IOLoop to
    schedule child greenlet for resumption when I/O is ready.

    We only implement those socket methods actually used by pymongo: connect,
    sendall, and recv.
    """
    def __init__(self, sock):
        self.socket = sock
        self._stream = None

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

    def close(self):
        self.stream.close()

    def __del__(self):
        self.close()

class TornadoPool(object):
    """A simple connection pool of TornadoSockets.
    """

    def __init__(self, max_size, net_timeout, conn_timeout, use_ssl):
        self.pid = os.getpid()
        self.max_size = max_size

        # TODO: how do connection- and net-timeouts work w/ non-blocking sockets?
        self.net_timeout = net_timeout
        self.conn_timeout = conn_timeout
        self.use_ssl = use_ssl
        self.sockets = []
        self.greenlet2socket = {}

    def connect(self, host, port):
        """Connect to Mongo and return a new connected socket.
        """
        assert greenlet.getcurrent().parent, "Should be on child greenlet"
        try:
            # Prefer IPv4. If there is demand for an option
            # to specify one or the other we can add it later.
            s = socket.socket(socket.AF_INET)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except socket.gaierror:
            # If that fails try IPv6
            s = socket.socket(socket.AF_INET6)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        tornado_sock = TornadoSocket(s)

        # TornadoSocket will pause the current greenlet and resume it when
        # connection has completed
        tornado_sock.connect((host, port))

        if self.use_ssl:
            try:
                # TODO: ugly, probably wrong
                tornado_sock.socket = ssl.wrap_socket(tornado_sock.socket)
            except ssl.SSLError:
                tornado_sock.close()
                raise ConnectionFailure("SSL handshake failed. MongoDB may "
                                        "not be configured with SSL support.")

        return tornado_sock

    def get_socket(self, host, port):
        # Unlike pymongo.Pool's get_socket(), we give out a socket here to
        # which the caller has exclusive access until it closes the socket or
        # calls return_socket().

        # We use the pid here to avoid issues with fork / multiprocessing.
        # See test.test_connection:TestConnection.test_fork for an example of
        # what could go wrong otherwise

        # TODO: comment above copied from official pymongo pool; does it apply
        # to async? If not, can we delete all pid-management?
        pid = os.getpid()

        if pid != self.pid:
            # TODO: refactor this into a function, __reset(), that's called from
            # init
            self.sockets = []
            self.greenlet2socket = {}
            self.pid = pid

        try:
            sock = (pid, self.sockets.pop())
            print >> sys.stderr, 'reusing', sock, 'on greenlet', greenlet.getcurrent()
            self.greenlet2socket[greenlet.getcurrent()] = sock
            return (sock[1], True)
        except IndexError:
            sock = (pid, self.connect(host, port))
            self.greenlet2socket[greenlet.getcurrent()] = sock
            return (sock[1], False)

    def return_socket(self):
        # TODO: what if a socket created in a parent process is returned here
        # after a fork?
        # TODO: also, what about multithreading PLUS greenlets? are we thread-
        # safe here?
        gr = greenlet.getcurrent()
        assert gr.parent, "Should be on child greenlet?"

        sock = self.greenlet2socket.get(gr)
        if sock:
            # There's a race condition here, but we deliberately
            # ignore it.  It means that if the pool_size is 10 we
            # might actually keep slightly more than that.
            if len(self.sockets) < self.max_size:
                self.sockets.append(sock)
            else:
                sock.close()
        else:
            print >> sys.stderr, "return_socket() called on", gr, "without associated socket"

class TornadoConnection(object):
    def __init__(self, *args, **kwargs):
        # Store args and kwargs for when open() is called
        self.__init_args = args
        self.__init_kwargs = kwargs

        # The synchronous pymongo Connection
        self.sync_connection = None
        self.connected = False

    def open(self, callback):
        """
        Actually connect, passing self to a callback when connected.
        @param callback: Optional function taking parameters (connection, error)
        """
        check_callable(callback)
        
        def connect():
            # Run on child greenlet
            error = None
            try:
                cx = self.sync_connection = pymongo.Connection(
                    *self.__init_args,
                    pool_class=TornadoPool,
                    **self.__init_kwargs
                )
                
                # Replace the standard _Pool with an TornadoSocket-producing pool
                # TODO: Don't require accessing private attributes
                cx._Connection__pool = TornadoPool(
                    cx.max_pool_size,
                    cx._Connection__net_timeout,
                    cx._Connection__conn_timeout,
                    cx._Connection__use_ssl
                )

                self.connected = True
            except Exception, e:
                error = e

            if callback:
                # Schedule callback to be executed on main greenlet, with
                # (self, None) if no error, else (None, error)
                tornado.ioloop.IOLoop.instance().add_callback(
                    lambda: callback(
                        None if error else self,
                        error
                    )
                )

        # Actually connect on a child greenlet
        greenlet.greenlet(connect).switch()

    def __getattr__(self, name):
        """Get a database by name.

        Raises :class:`~pymongo.errors.InvalidName` if an invalid
        database name is used.

        :Parameters:
          - `name`: the name of the database to get
        """
        if not self.connected:
            raise InvalidOperation(
                "Can't access database on TornadoConnection before calling"
                " connect()"
            )
        return TornadoDatabase(self.sync_connection, name)
    
    def __getitem__(self, name):
        """Get a database by name.

        Raises :class:`~pymongo.errors.InvalidName` if an invalid
        database name is used.

        :Parameters:
          - `name`: the name of the database to get
        """
        return self.__getattr__(name)
    
    def __repr__(self):
        return 'TornadoConnection(%s)' % (
            ','.join([
                i for i in [
                    ','.join(self.__init_args),
                    ','.join(self.__init_kwargs),
                ] if i
            ])
        )

class TornadoDatabase(pymongo.database.Database):
    def __getattr__(self, collection_name):
        """
        Return an async Collection instead of a pymongo Collection
        """
        return TornadoCollection(self, collection_name)

    def command(self, command, value=1,
                check=True, allowable_errors=[],
                uuid_subtype=UUID_SUBTYPE, callback=None, **kwargs):
        # TODO: What semantics exactly shall we support with check and callback?
        #   Is check still necessary to support the pymongo API internally ... ?
        if check and not callback:
            raise InvalidOperation("Must pass a callback if check is True")

        check_callable(callback)

        if isinstance(command, basestring):
            command = SON([(command, value)])

        use_master = kwargs.pop('_use_master', True)

        fields = kwargs.get('fields')
        if fields is not None and not isinstance(fields, dict):
            kwargs['fields'] = helpers._fields_list_to_dict(fields)

        command.update(kwargs)

        def command_callback(result, error):
            # TODO: what's the diff b/w getting an error here and getting one in
            # _check_command_response?
            if error:
                if callback:
                    callback(result, error)
            elif check:
                msg = "command %s failed: %%s" % repr(command).replace("%", "%%")
                try:
                    # TODO: test if disconnect() is called correctly
                    helpers._check_command_response(result, self.connection.disconnect,
                        msg, allowable_errors)

                    # No exception thrown
                    callback(result, error)
                except Exception, e:
                    callback(result, e)

        self["$cmd"].find_one(command,
                              _must_use_master=use_master,
                              _is_command=True,
                              _uuid_subtype=uuid_subtype,
                              callback=command_callback)
 
    def __repr__(self):
        return 'Tornado' + super(TornadoDatabase, self).__repr__()

class TornadoCollection(pymongo.collection.Collection):
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
        super_obj = super(TornadoCollection, self)
        sync_method = super_obj.__getattribute__(operation_name)

        if operation_name not in ('update', 'insert', 'remove'):
            return sync_method
        else:
            def method(*args, **kwargs):
                client_callback = kwargs.get('callback')
                check_callable(client_callback)

                if 'callback' in kwargs:
                    kwargs = kwargs.copy()
                    del kwargs['callback']

                kwargs['safe'] = bool(client_callback)

                def call_method():
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

                # Start running the operation on greenlet
                greenlet.greenlet(call_method).switch()

            return method

    def save(self, to_save, manipulate=True, safe=False, **kwargs):
        """Save a document in this collection."""
        if not isinstance(to_save, dict):
            raise TypeError("cannot save object of type %s" % type(to_save))

        if "_id" not in to_save:
            return self.insert(to_save, manipulate, safe=safe, **kwargs)
        else:
            client_callback = kwargs.get('callback')
            check_callable(client_callback)
            if 'callback' in kwargs:
                kwargs = kwargs.copy()
                del kwargs['callback']

            if client_callback:
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
        Run an async find(), and return a TornadoCursor, rather than returning a
        pymongo Cursor for synchronous operations.
        """
        client_callback = kwargs.get('callback')
        check_callable(client_callback, required=True)
        kwargs = kwargs.copy()
        del kwargs['callback']

        cursor = super(TornadoCollection, self).find(*args, **kwargs)
        tornado_cursor = TornadoCursor(cursor)
        tornado_cursor.get_more(client_callback)

        # When the greenlet has sent the query on the socket, it will switch
        # back to the main greenlet, here, and we return to the caller.
        return tornado_cursor

    def find_one(self, *args, **kwargs):
        client_callback = kwargs.get('callback')
        check_callable(client_callback, required=True)
        
        if 'callback' in kwargs:
            kwargs = kwargs.copy()
            del kwargs['callback']

        if 'limit' in kwargs:
            raise TypeError("'limit' argument not allowed for find_one")

        def find_one_callback(result, error):
            # Turn single-document list into a plain document.
            # This is run on the main greenlet.
            assert result is None or len(result) == 1, (
                "Got %d results from a findOne" % len(result)
            )

            client_callback(result[0] if result else None, error)

        # TODO: python2.4-compatible?
        self.find(*args, limit=-1, callback=find_one_callback, **kwargs)

    def __repr__(self):
        return 'Tornado' + super(TornadoCollection, self).__repr__()


class TornadoCursor(object):
    def __init__(self, cursor):
        """
        @param cursor:  Synchronous pymongo.Cursor
        """
        self.__sync_cursor = cursor
        self.started = False

    def get_more(self, callback):
        """
        Get a batch of data asynchronously, either performing an initial query
        or getting more data from an existing cursor.
        @param callback:    Optional function taking parameters (result, error)
        """
        check_callable(callback)
        assert not self.__sync_cursor._Cursor__killed
        if self.started and not self.alive:
            raise InvalidOperation(
                "Can't call get_more() on an TornadoCursor that has been"
                " exhausted or killed."
            )

        def _get_more():
            # This is executed on child greenlet
            result, error = None, None
            try:
                self.started = True
                self.__sync_cursor._refresh()

                # TODO: Make this accessible w/o underscore hack
                result = self.__sync_cursor._Cursor__data
                self.__sync_cursor._Cursor__data = []
            except Exception, e:
                error = e

            # Execute callback on main greenlet
            tornado.ioloop.IOLoop.instance().add_callback(
                lambda: callback(result, error)
            )

        greenlet.greenlet(_get_more).switch()

        # When the greenlet has sent the query on the socket, it will switch
        # back to the main greenlet, here, and we return to the caller.
        return None

    @property
    def alive(self):
        """Does this cursor have the potential to return more data?"""
        return self.__sync_cursor.alive and self.__sync_cursor._Cursor__id

    def close(self):
        """Explicitly close this cursor.
        """
        greenlet.greenlet(self.__sync_cursor.close).switch()
