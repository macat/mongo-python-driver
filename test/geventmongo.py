# Copyright (C) 2011 by Antonin Amand @gwik <antonin.amand@gmail.com>
# 
# Part of this source code is from the original pymongo distribution by 10gen.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

import os
import pymongo.connection
from gevent.hub import getcurrent
import gevent.queue
import gevent.greenlet
import gevent.local
import gevent.coros
from gevent import socket
import weakref


class Pool(object):
    """ A greenlet safe connection pool for gevent (non-thread safe).
    """

    DEFAULT_TIMEOUT = 3.0

    def __init__(self, pool_size, network_timeout=None, *args, **kwargs):
        self.network_timeout = network_timeout or self.DEFAULT_TIMEOUT
        self.pool_size = pool_size
        self._bootstrap(os.getpid())
        self._lock = gevent.coros.RLock()

    def _bootstrap(self, pid):
        self._count = 0
        self._pid = pid
        self._used = {}
        self._queue = gevent.queue.Queue(self.pool_size)

    def connect(self, host, port):
        """Connect to Mongo and return a new (connected) socket.
        """
        try:
            # Prefer IPv4. If there is demand for an option
            # to specify one or the other we can add it later.
            s = socket.socket(socket.AF_INET)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            s.settimeout(self.network_timeout or 
                    pymongo.connection._CONNECT_TIMEOUT)
            s.connect((host, port))
            s.settimeout(self.network_timeout)
            return s
        except socket.gaierror:
            # If that fails try IPv6
            s = socket.socket(socket.AF_INET6)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            s.settimeout(self.network_timeout or 
                    pymongo.connection._CONNECT_TIMEOUT)
            s.connect((host, port))
            s.settimeout(self.network_timeout)
            return s

    def get_socket(self, host, port):
        pid = os.getpid()
        if pid != self._pid:
            self._bootstrap(pid)

        greenlet = getcurrent()
        from_pool = True
        sock = self._used.get(greenlet)
        if sock is None:
            try:
                self._lock.acquire()
                if self._count < self.pool_size:
                    self._count += 1
                    from_pool = False
                    sock = self.connect(host, port)
            finally:
                self._lock.release()
        if sock is None:
            from_pool = True
            sock = self._queue.get(timeout=self.network_timeout)

        if isinstance(greenlet, gevent.Greenlet):
            greenlet.link(self._return)
            self._used[greenlet] = sock
        else:
            ref = weakref.ref(greenlet, self._return)
            self._used[ref] = sock
        return sock, from_pool

    def return_socket(self):
        greenlet = getcurrent()
        self._return(greenlet)

    def _return(self, greenlet):
        try:
            sock = self._used.get(greenlet)
            if sock is not None:
                del self._used[greenlet]
                self._queue.put(sock)
        except:
            try:
                self._lock.acquire()
                self._count -= 1
            finally:
                self._lock.release()


def patch():
    import pymongo.connection
    pymongo.connection._Pool = Pool

