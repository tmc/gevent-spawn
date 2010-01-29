import os
import sys
import time
import signal

import gevent
from gevent import core
from gevent.wsgi import WSGIServer

from gevent_spawn import signal_stop
from gevent_spawn.worker import Worker
from gevent_spawn.ipc import WorkerController
from gevent_spawn.utils import setproctitle, signum2name

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
        self.log_server("Master of disaster, PID " + str(os.getpid()))
        self.stop_event.wait()
        self.log_server("Stop event")

    def start(self):
        """Set up signal actions."""
        setproctitle("gevent-spawn.py: master")
        core.signal(signal.SIGCHLD, self._cb_sigchld)
        core.signal(signal.SIGHUP, self._cb_sighup)
        core.signal(signal.SIGTERM, self._cb_sigterm)

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

    def _cb_sigterm(self):
        """Initiate stopping sequence"""
        self.log_server("SIGTERM, initiating stop")
        gevent.spawn(self.stop)

    def handle_worker_status(self, pid, status):
        """Respawn worker in place of *pid* and update the list of workers."""
        pdesc = "PID %d" % pid
        if os.WIFCONTINUED(status):
            # Because these are unreliable (only even get them on Linux), and
            # we don't really need them in any way, ignore them.
            return
        elif os.WIFSTOPPED(status):
            signame = signum2name(os.WSTOPSIG(status))
            self.log_server("%s stopped by signal %s" % (pdesc, signame))
        elif os.WIFSIGNALED(status):
            signame = signum2name(os.WTERMSIG(status))
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
