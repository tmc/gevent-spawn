import os
import sys
import socket
from functools import partial

from gevent import core
from gevent.wsgi import WSGIServer, WSGIHandler

from gevent_spawn import signal_stop
from gevent_spawn.utils import setproctitle

class AccessLogWSGIHandler(WSGIHandler):

    def __init__(self, *args, **kwds):
        self.access_log = kwds.pop("access_log", None)
        super(AccessLogWSGIHandler, self).__init__(*args, **kwds)

    def log_request(self, *args):
        out = getattr(self, "access_log", None)
        print >>out, self.format_request(*args)

class Worker(WSGIServer):
    """Plain WSGI server with misc. worker-related additions."""

    handler_class = AccessLogWSGIHandler
    base_env = WSGIServer.base_env.copy()
    base_env["wsgi.multiprocess"] = True

    def __init__(self, sock, application, backlog=24, access_log=None,
                 *args, **kwds):
        address = sock.getsockname()
        super(Worker, self).__init__(address, application, *args, **kwds)
        self.socket = sock
        self.backlog = backlog
        self.access_log = access_log
        self.handler_class = partial(self.handler_class, access_log=access_log)

    def _cb_stop_worker(self):
        self.log_server("Stop signal")
        self._stopped_event.set()

    def start(self):
        setproctitle("gevent-spawn.py: worker")

        super(Worker, self).start()
        core.signal(signal_stop, self._cb_stop_worker)
        env = self.base_env.copy()
        env["SERVER_NAME"] = socket.getfqdn(self.server_host)
        env["SERVER_PORT"] = str(self.server_port)
        self.base_env = env
        self.log_server("Worker serving on %s:%d" %
                        (self.server_host, self.server_port))
        return self.socket

    def handle_notification(self, msg):
        if not msg:
            self.log_server("Master seems to have died, harakiri")
            self._stopped_event.set()
        elif msg == "stop":
            self.log_server("Master says stop")
            self._stopped_event.set()
        elif msg == "hello":
            self.log_server("Master says hi")  # DEBUG
        else:
            self.log_server("Unknown notify: " + repr(msg))

    @property
    def ident(self):
        return str(os.getpid())

    def log_server(self, msg):
        print >>sys.stderr, "%8s %s" % (self.ident, msg)
