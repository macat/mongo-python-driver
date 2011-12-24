import sys
import unittest
from tornado import ioloop

class PuritanicalIOLoop(ioloop.IOLoop):
    """
    A loop that quits when it encounters an Exception.
    """
    def handle_callback_exception(self, callback):
        exc_type, exc_value, tb = sys.exc_info()
        raise exc_value

class PuritanicalTest(unittest.TestCase):
    def setUp(self):
        super(PuritanicalTest, self).setUp()

        # Clear previous loop
        if ioloop.IOLoop.initialized():
            loop = ioloop.IOLoop.instance()
            if loop:
                loop.stop()
            del ioloop.IOLoop._instance

        # So any function that calls IOLoop.instance() gets the
        # PuritanicalIOLoop instead of the default loop.
        loop = PuritanicalIOLoop()
        loop.install()
