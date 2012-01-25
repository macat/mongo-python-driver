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

"""
Like pool.Pool, but start_request() binds a socket to this greenlet, as well
as this thread, until end_request().
"""

import greenlet
import pool

class GreenletPool(pool.Pool):
    def _request_key(self):
        super_key = pool.Pool._request_key(self)
        return (super_key, greenlet.getcurrent())