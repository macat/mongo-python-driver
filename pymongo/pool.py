# Copyright 2009-2011 10gen, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

import os
import socket
import threading
import sys

from pymongo.errors import ConnectionFailure

have_ssl = True
try:
    import ssl
except ImportError:
    have_ssl = False


class Pool(object):
    """A simple connection pool.

    # TODO: document start / end request, and what 'pair' means
    """
    def __init__(self, pair, max_size, net_timeout, conn_timeout, use_ssl):
        self._reset()

        self.pair = pair
        self.max_size = max_size
        self.net_timeout = net_timeout
        self.conn_timeout = conn_timeout
        self.use_ssl = use_ssl

    def _reset(self):
        print >> sys.stderr, 'reset'
        self.pid = os.getpid()
        self.sockets = set()

        # Map socket -> set of (host, port, database) triples for which this
        # socket has been authorized
        # TODO: make sure this works and unittest that we don't unnecessarily
        # re-authorize sockets that are already authorized
        self.authorizations = {}

        # Map thread -> socket, or (thread, greenlet) -> socket
        self.requests = {}

    def _request_key(self):
        """Overridable"""
        return id(threading.current_thread())

    def connect(self, pair):
        """Connect to Mongo and return a new (connected) socket.
        """
        # TODO: remove, or explain, this pair override business
        assert self.pair is None or pair is None

        # Prefer IPv4. If there is demand for an option
        # to specify one or the other we can add it later.
        socket_types = (socket.AF_INET, socket.AF_INET6)
        for socket_type in socket_types:
            try:
                s = socket.socket(socket_type)
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                s.settimeout(self.conn_timeout or 20.0)
                s.connect(self.pair or pair)
                break
            except socket.gaierror:
                # If that fails try IPv6
                continue
        else:
            # None of the socket types worked
            raise

        if self.use_ssl:
            try:
                s = ssl.wrap_socket(s)
            except ssl.SSLError:
                s.close()
                raise ConnectionFailure("SSL handshake failed. MongoDB may "
                                        "not be configured with SSL support.")

        s.settimeout(self.net_timeout)

        print >> sys.stderr, 'created', s
        return s

    def get_socket(self, pair):
        """Get a socket from the pool. Returns a new socket if the
        pool is empty.
        # TODO: document return value
        # TODO: search for all calls to get_socket() and make sure they're
          matched by return_socket()
        """
        # TODO: remove, or explain, this pair override business
        assert self.pair is None or pair is None

        # We use the pid here to avoid issues with fork / multiprocessing.
        # See test.test_connection:TestConnection.test_fork for an example of
        # what could go wrong otherwise
        if self.pid != os.getpid():
            self._reset()

        # Have we called start_request() before?
        request_key = self._request_key()
        sock = self.requests.get(request_key)
        if sock:
            return sock, True

        # We're not in a request, just get any free socket or create one
        sock, from_pool = None, None
        try:
            sock, from_pool = self.sockets.pop(), True
        except KeyError:
            sock, from_pool = self.connect(pair), False

        if request_key in self.requests:
            # self.requests[request_key] is None, so someone called
            # start_request but didn't provide a socket. Let's use this socket
            # for this request until end_request.
            self.requests[request_key] = sock

        print >> sys.stderr, sock, 'from_pool', from_pool, self._request_key()
        return sock, from_pool

    def start_request(self, sock=None):
        # TODO: explain
        # TODO: test with and without a sock provided
        # Have we called start_request() before?
        request_key = self._request_key()
        request_sock = self.requests.get(request_key)
        if sock or not request_sock:
            self.requests[request_key] = sock

    def end_request(self):
        request_key = self._request_key()
        request_sock = self.requests.get(request_key)
        if request_sock:
            self.return_socket(request_sock)

        # Even if request_sock is None, it could mean someone called
        # start_request with sock=None
        if request_key in self.requests:
            del self.requests[request_key]

    def discard_socket(self, sock):
        """Close and discard the active socket.
        """
        print >> sys.stderr, 'discard', sock
        sock.close()
        request_key = self._request_key()
        request_sock = self.requests.get(request_key)
        if request_sock == sock:
            del self.requests[request_key]

    def return_socket(self, sock):
        """Return the socket currently in use to the pool. If the
        pool is full the socket will be discarded.
        """
        if self.pid != os.getpid():
            self._reset()
        elif sock:
            # There's a race condition here, but we deliberately
            # ignore it.  It means that if the pool_size is 10 we
            # might actually keep slightly more than that.
            if len(self.sockets) < self.max_size:
                print >> sys.stderr, 'returned', sock
                if sock in self.sockets:
                    pass
                assert sock not in self.sockets
                self.sockets.add(sock)
            else:
                print >> sys.stderr, 'closing', sock
                sock.close()
