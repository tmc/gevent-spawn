"""Inter-process communication helpers."""

import os
import gevent
from gevent import core

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
