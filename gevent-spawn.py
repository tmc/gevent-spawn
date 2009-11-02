#!/usr/bin/env python
"""Spawn WSGI applications using a gevent-based HTTP daemon.

Architecture
============

The architecture is a simple master-worker type of thing. Each worker has a
read pipe, and a write pipe from the master. The master handles SIGCHLD and
restarts dead workers, and so on.

Only one listening socket is set up, then libevent (via gevent) is used to
asynchronously accept connections, read the data and write the response.

Dependencies
============

 * Python 2.6 (I'd guess)
 * gevent
 * python-daemon
"""

import os
import time
import errno
import socket
import signal
import optparse
from contextlib import closing
from functools import partial

import gevent
from gevent import core
from gevent.http import HTTPServer
from gevent.wsgi import WSGIServer, WSGIHandler
from daemon import DaemonContext
from daemon.pidlockfile import PIDLockFile

try:
    from procname import setprocname
except ImportError:
    def setprocname(name): pass

# An ugly way to do this, but it works well.
signame_map = dict((v, a) for (a, v) in vars(signal).items()
                   if a.startswith("SIG"))
signame_lookup = lambda s: signame_map.get(s, str(s))

O = optparse.make_option

# {{{ Factories
class BaseFactory(object):
    @classmethod
    def args_sufficient(cls, opts, args):
        if hasattr(cls, "num_args"):
            return len(args) == cls.num_args
        return True

    @classmethod
    def configure_parser(cls, parser):
        parser.set_usage("%prog [options] " + cls.args_desc)
        parser.add_options(getattr(cls, "opts", ()))

def load_app_spec(spec):
    if ":" in spec:
        modname, attname = spec.split(":", 1)
    else:
        modname, attname = spec, "application"
    call = False
    if attname.endswith("()"):
        attname, call = attname[:-2], True
    mod = __import__(modname, fromlist=[attname])
    rv = getattr(mod, attname)
    if call:
        rv = rv()
    return rv

class WSGIFactory(BaseFactory):
    """WSGI application from a path.to.module[:attr] specification."""
    num_args = 1
    args_desc = "<app qualifier>"

    @classmethod
    def make_wsgi_app(cls, opts, spec):
        return load_app_spec(spec)

try:
    from django.conf import settings as django_settings, \
                            Settings as DjangoSettings
except ImportError:
    _django_loaded = False
else:
    _django_loaded = True

class DjangoFactory(BaseFactory):
    """Django application from a settings module specification."""
    num_args = 1
    args_desc = "<settings module qualifier>"

    wsgi_app = "django.core.handlers.wsgi:WSGIHandler()"
    opts = [O("--force-settings", dest="force_settings", action="store_true",
              help="Force settings to be configured."),
            O("--wsgi-app", dest="wsgi_app", metavar="APP", default=wsgi_app,
              help="Use WSGI application to run Django.")]

    @classmethod
    def make_wsgi_app(cls, opts, spec):
        if opts.force_settings or not django_settings.configured:
            django_settings._wrapped = DjangoSettings(spec)
        elif django_settings.configured:
            if django_settings.SETTINGS_MODULE != spec:
                raise ValueError("django already configured for %s" %
                                 django_settings.SETTINGS_MODULE)
        return load_app_spec(opts.wsgi_app)

factories = {"wsgi": WSGIFactory, "django": DjangoFactory}

if not _django_loaded:
    factories.pop("django")
# }}}

# {{{ Options
parser = optparse.OptionParser()
A = parser.add_option
A("-f", "--factory", dest="factory", default="wsgi", metavar="NAME",
  help="Use NAME to spawn WSGI application. (default: wsgi)\n"
       "Note that this option needs to be the first argument if set.\n"
       "Using ? as NAME lists available factories.")
A("-i", "--host", dest="addr", default="127.0.0.1", metavar="ADDR",
  help="IP address (interface) to bind to (default: 127.0.0.1)")
A("-p", "--port", dest="port", default=8000, type=int, metavar="PORT",
  help="TCP port to bind to (default: 8000)")

G = optparse.OptionGroup(parser, "Performance and concurrency")
A = G.add_option
parser.add_option_group(G)
A("--backlog", dest="backlog", type=int, default=5, metavar="NUM",
  help="Set kernel listen queue to size NUM.")
A("-w", "--workers", "--processes", dest="num_procs", type=int, default=1,
  metavar="NUM", help="Fork to NUM processes accepting on the same socket.")

# TODO Implement this option group.
G = optparse.OptionGroup(parser, "Logging")
A = G.add_option
parser.add_option_group(G)
A("--error-log", dest="error_log", default="-", metavar="FILE",
  help="Write error (status) log to FILE. (default: -, meaning stderr)")
A("--access-log", dest="access_log", default="-", metavar="FILE",
  help="Write access log to FILE. (default: -, meaning stderr)")

# TODO Implement this option group.
G = optparse.OptionGroup(parser, "Daemonizing")
A = G.add_option
parser.add_option_group(G)
A("-d", "--detach", dest="detach", action="store_true",
  help="Detach from controlling terminal.")
A("--pidfile", dest="pidfile", metavar="FILE",
  help="Write master PID to FILE.")
A("--chroot", dest="chroot_dir", metavar="DIR",
  help="Change root directory to DIR (chroot).")
A("--user", dest="set_user", metavar="USER", help="Change to USER.")
A("--group", dest="set_group", metavar="GROUP", help="Change to GROUP.")
# }}}

signal_reload = signal.SIGHUP
signal_stop = signal.SIGUSR1

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
        self.sock = sock
        self.backlog = backlog
        self.access_log = access_log
        self.handler_class = partial(self.handler_class, access_log=access_log)

    def _cb_stop_worker(self):
        self.log_server("Stop signal")
        self._stopped_event.set()

    def start(self):
        setprocname("gevent-spawn.py: worker")
        HTTPServer.start(self, self.sock)
        core.signal(signal_stop, self._cb_stop_worker)
        env = self.base_env.copy()
        env["SERVER_NAME"] = socket.getfqdn(self.server_host)
        env["SERVER_PORT"] = str(self.server_port)
        self.base_env = env
        self.log_server("Worker serving on %s:%d" %
                        (self.server_host, self.server_port))
        return self.sock

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

class WorkerController(object):
    """Master's channel to a worker pipe (and PID)."""

    def __init__(self, pid, pipe_r=None, pipe_w=None):
        self._pid = pid
        if pipe_r and not hasattr(pipe_r, "fileno"):
            pipe_r = os.fdopen(pipe_r, "r")
        if pipe_w and not hasattr(pipe_w, "fileno"):
            pipe_w = os.fdopen(pipe_w, "w")
        self.rfile = pipe_r
        self.wfile = pipe_w
        self.in_buff = []
        self.out_buff = []

    def __str__(self):
        return "Worker PID #%d" % self._pid

    def __repr__(self):
        a = self._pid, self.wfile.fileno(), self.rfile.fileno()
        return "<%s of %s (%d, %d)>" % ((self.__class__.__name__,) + a)
    
    def __eq__(self, other):
        if hasattr(other, "pid"):
            other = other.pid
        return self._pid == other

    def __cmp__(self, other):
        if hasattr(other, "pid"):
            other = other.pid
        return cmp(self._pid, other)

    def __hash__(self):
        return hash(self._pid)

    @property
    def pid(self):
        return self._pid

    def notify(self, msg):
        self.out_buff.append(msg + "\n")
        core.write_event(self.wfile.fileno(), self._cb_notify_write)

    def _cb_notify_write(self, ev, evtype):
        if not any(self.out_buff):
            self.out_buff[:] = []
            ev.cancel()
        else:
            data = "".join(self.out_buff)
            wrote = os.write(ev.fd, data)
            data = data[wrote:]
            self.out_buff[:] = [data]

    def begin_notify_receive(self, callback):
        e = core.event(core.EV_READ | core.EV_PERSIST,
                       self.rfile.fileno(), self._cb_notify_read,
                       arg=callback)
        e.add()

    def _cb_notify_read(self, ev, evtype):
        chunk = os.read(self.rfile.fileno(), 4096)
        if not chunk:
            gevent.spawn(ev.arg, chunk)
            ev.cancel()
            return
        self.in_buff.append(chunk)
        if "\n" in chunk:
            notifies = "".join(self.in_buff).split("\n")
            self.in_buff[:] = [notifies.pop()]
            for msg in notifies:
                gevent.spawn(ev.arg, msg)

class Master(object):
    base_env = WSGIServer.base_env.copy()
    exiting = False
    pidfile = None

    exit_stopped = 0x10

    def __init__(self, address, wsgi_app):
        # Convenient way of creating a socket. \o/
        self.sock = WSGIServer(address, wsgi_app).make_listener(address)
        self.address = address
        self.application = wsgi_app
        self.num_workers = 0
        self.workers = set()
        self.worker_args = (), {}
        self.last_spawn = None
        self.exit_completed = gevent.event.Event()
        self.stop_event = gevent.event.Event()

    def configure_workers(self, *args, **kwds):
        """Set the initialization arguments for the Worker class."""
        self.worker_args = args, kwds

    def spawn_workers(self, num_workers=1):
        """Spawn *num_workers* workers."""
        if not num_workers:
            raise ValueError("must have at least one process")
        self.num_workers += num_workers
        for i in xrange(num_workers):
            self.start_worker()

    def serve_forever(self):
        self.start()
        self.log_server("Master of disaster")
        self.stop_event.wait()
        self.log_server("Stop event")

    def start(self):
        """Set up signal actions."""
        setprocname("gevent-spawn.py: master")
        core.signal(signal.SIGCHLD, self._cb_sigchld)
        core.signal(signal.SIGHUP, self._cb_sighup)

    def start_worker(self, sock=None):
        """Start a worker on *sock* or *self.sock* and add to list."""
        if self.stop_event.is_set():
            raise RuntimeError("start_worker during stop_event")
        sock = sock if sock else self.sock
        pipe_r, pipe_w = os.pipe()
        pid = gevent.fork()
        if pid:
            os.close(pipe_r)
            cntr = WorkerController(pid, pipe_w=pipe_w)
            self.workers.add(cntr)
            self.exit_completed.clear()
            cntr.notify("hello")
            return pid
        os.close(pipe_w)
        w_args, w_kwds = self.worker_args
        worker = Worker(self.sock, self.application, *w_args, **w_kwds)
        cntr = WorkerController(os.getpid(), pipe_r=pipe_r)
        cntr.begin_notify_receive(worker.handle_notification)
        try:
            worker.serve_forever()
        except KeyboardInterrupt:
            pass
        os._exit(self.exit_stopped)

    def _cb_sigchld(self):
        """Handle child status update signal ("update" being "death")."""
        while self.workers:
            pid, status = os.waitpid(-1, os.WNOHANG | os.WUNTRACED)
            if not pid:
                break
            elif pid not in self.workers:
                self.log_server("Oops! Reaped PID %d, which wasn't "
                                "a known child of ours" % pid)
                continue
            self.workers.remove(pid)
            gevent.spawn(self.handle_worker_status, pid, status)

    def _cb_sighup(self):
        """Stop all children and thus spawn new."""
        self.log_server("SIGHUP, stopping children")
        gevent.spawn(self.notify_workers, "stop")

    def handle_worker_status(self, pid, status):
        """Respawn worker in place of *pid* and update the list of workers."""
        pdesc = "PID %d" % pid
        if os.WIFCONTINUED(status):
            # Because these are unreliable (only even get them on Linux), and
            # we don't really need them in any way, ignore them.
            return
        elif os.WIFSTOPPED(status):
            signame = signame_lookup(os.WSTOPSIG(status))
            self.log_server("%s stopped by signal %s" % (pdesc, signame))
        elif os.WIFSIGNALED(status):
            signame = signame_lookup(os.WTERMSIG(status))
            self.log_server("%s terminated by signal %s" % (pdesc, signame))
        elif os.WIFEXITED(status):
            rc = os.WEXITSTATUS(status)
            if rc == self.exit_stopped:
                self.log_server("%s exited (stopped)" % pdesc)
            else:
                self.log_server("%s exited with exit status %d" % (pdesc, rc))
        if self.stop_event.is_set():
            if not self.workers:
                self.exit_completed.set()
            return
        # Compensate for our lost child by making a new worker child.
        while self.last_spawn == int(time.time()):
            self.log_server("Sleeping due to high spawn rate")
            gevent.sleep(1)
        self.last_spawn = int(time.time())
        gevent.spawn(self.start_worker)

    def stop(self, timeout=1):
        """Issue a stop event, and wait for the stop to complete."""
        self.log_server("Stopping")
        self.stop_event.set()
        self.exiting = True
        gevent.spawn(self.notify_workers, "stop")
        gevent.spawn_later(1, self.kill_workers)
        self.exit_completed.wait(timeout=timeout)
        if not self.exit_completed:
            wpdesc = ", ".join(map(str, self.workers))
            self.log_server("Forcefully killing workers: " + wpdesc)
            self.kill_workers(signal.SIGTERM)

    def notify_workers(self, *args):
        """Send a notification to all workers."""
        for worker in self.workers:
            worker.notify(*args)

    def kill_workers(self, signum=signal_stop):
        """Send a signal to all workers."""
        for worker in self.workers:
            os.kill(worker.pid, signum)

    @property
    def ident(self):
        if self.num_workers > 1:
            return "%d-master" % os.getpid()
        else:
            return "master"

    def log_server(self, msg):
        print >>sys.stderr, "%8s %s" % (self.ident, msg)

def main(argv, exec_argv):
    # This fine hack to be able to inject optparse options before parsing.
    if argv and (argv[0] in ("-f", "--factory") or argv[0].startswith("--factory=")):
        factory_opt = argv.pop(0)
        if "=" in factory_opt:
            factory_name = factory_opt.split("=", 1)[1]
        else:
            factory_name = argv.pop(0)
    else:
        factory_name = "wsgi"
    if factory_name not in factories:
        factory_help = "".join(" * %s: %s\n" % (k, f.__doc__.split("\n", 1)[0])
                               for k, f in factories.iteritems())
        if factory_name == "?":
            parser.exit(0, factory_help)
        else:
            parser.error("unknown factory\n" + factory_help[:-1])
    factory = factories[factory_name]
    factory.configure_parser(parser)
    # Parse options.
    opts, args = parser.parse_args(args=argv)
    if not factory.args_sufficient(opts, args):
        parser.error("insufficient arguments for factory")
    wsgi_app = factory.make_wsgi_app(opts, *args)
    master = Master((opts.addr, opts.port), wsgi_app)
    # Set up daemon context
    status_log = sys.stderr
    if opts.error_log != "-":
        status_log = open(opts.error_log, "a+")
    access_log = sys.stdout
    if opts.access_log != "-":
        access_log = open(opts.access_log, "a+")
    master.configure_workers(backlog=opts.backlog, access_log=access_log)
    dctx = DaemonContext(stdout=status_log, stderr=status_log)
    dctx.files_preserve = [master.sock, access_log]
    dctx.detach_process = opts.detach
    dctx.chroot_directory = opts.chroot_dir
    # Better to explicitly state that no signal mapping should be done; we do
    # this with libevent and I'd think these interfere with eachother.
    # TODO Subclass DaemonContext and make the signal map use gevent?
    dctx.signal_map = {}
    if opts.pidfile:
        dctx.pidfile = PIDLockFile(opts.pidfile, threaded=False)
        print "pidfile =", dctx.pidfile
    if opts.set_user:
        from pwd import getpwnam
        dctx.uid = getpwnam(user).pw_uid
    if opts.set_group:
        from grp import getgrnam
        dctx.gid = getgrnam(group).gr_gid
    # Go!
    with dctx:
        gevent.reinit()  # Needs to be done as dctx might've forked.
        master.spawn_workers(opts.num_procs)
        try:
            master.serve_forever()
        except KeyboardInterrupt:
            master.stop()

if __name__ == "__main__":
    import sys
    main(sys.argv[1:], [sys.executable] + sys.argv[1:])
