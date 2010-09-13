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

DJANGO_HANDLER = 'django.core.handlers.wsgi:WSGIHandler()'

__usage__ = """%%prog [options] <wsgi app>

The program accepts at most one argument, a wsgi app to run.
It should be of form MODULE[:ATTR[()]], where
 - MODULE is a Python module or a path to a Python script.
 - ATTR is an attribute of the module that is used as an application.
   If ATTR is followed by '()', that object is called first, and then
   the return value of the call is used.

Examples:

   # wsgiapp.py defines 'application' that is used
   %%prog examples/wsgiapp.py

   %%prog myproject.mypackage.myapp:WSGIApp()

   # <wsgi app> defaults to %s
   # when --django-settings option is provided or DJANGO_SETTINGS_MODULE environment variable is set.
   %%prog --django-settings mysite.myapp.settings
   DJANGO_SETTINGS_MODULE=mysite.myapp.settings %%prog
""" % DJANGO_HANDLER

import os
from optparse import OptionParser, OptionGroup

import gevent
from daemon import DaemonContext
from daemon.pidlockfile import PIDLockFile

from gevent_spawn.utils import import_filename
from gevent_spawn.master import Master

def load_app_spec(spec):
    if ":" in spec:
        modname, attname = spec.split(":", 1)
    else:
        modname, attname = spec, "application"
    call = False
    if attname.endswith("()"):
        attname, call = attname[:-2], True
    if os.path.isfile(modname):
        try:
            mod = import_filename(modname)
        except IOError, ex:
            sys.exit('Cannot load %r: %s.' % (modname, ex))
    else:
        if '/' in modname or '\\' in modname:
            sys.exit('Not a file: %r.' % (modname, ))
        try:
            mod = __import__(modname, fromlist=[attname])
        except ImportError, ex:
            sys.exit('Cannot load %r: %s.' % (modname, ex))
    if mod is None:
        sys.exit('Cannot load %r.' % modname)
    try:
        rv = getattr(mod, attname)
    except AttributeError:
        sys.exit('Module %r has no attribute %r.' % (modname, attname))
    if call:
        rv = rv()
    return rv

# {{{ Options
parser = OptionParser()
parser.set_usage(__usage__)
parser.add_option("-i", "--host", default="127.0.0.1", metavar="ADDR",
                  help="IP address (interface) to bind to (default: 127.0.0.1)")
parser.add_option("-p", "--port", dest="port", default=8000, type=int, metavar="PORT",
                  help="TCP port to bind to (default: 8000)")

group = OptionGroup(parser, "Django web framework")
parser.add_option_group(group)
group.add_option('--django-settings', help="Set DJANGO_SETTINGS_MODULE environment variable")

group = OptionGroup(parser, "Performance and concurrency")
parser.add_option_group(group)
group.add_option("--backlog", type=int, default=5, metavar="NUM",
                 help="Set kernel listen queue to size NUM.")
group.add_option("-w", "--workers", "--processes", type=int, default=1,
                 metavar="NUM", help="Fork to NUM processes accepting on the same socket.")

# TODO Implement this option group.
group = OptionGroup(parser, "Logging")
parser.add_option_group(group)
group.add_option("--error-log", default="-", metavar="FILE",
                 help="Write error (status) log to FILE. (default: -, meaning stderr)")
group.add_option("--access-log", default="-", metavar="FILE",
                 help="Write access log to FILE. (default: -, meaning stderr)")

group = OptionGroup(parser, "Daemonizing")
parser.add_option_group(group)
group.add_option("-d", "--detach", action="store_true",
                 help="Detach from controlling terminal.")
group.add_option("--pidfile", metavar="FILE",
                 help="Write master PID to FILE.")
group.add_option("--chroot", metavar="DIR",
                 help="Change root directory to DIR (chroot).")
group.add_option("--user", metavar="USER", help="Change to USER.")
group.add_option("--group", metavar="GROUP", help="Change to GROUP.")
# }}}

def main(argv, exec_argv):
    opts, args = parser.parse_args(args=argv)

    ENVIRONMENT_VARIABLE = "DJANGO_SETTINGS_MODULE"

    if opts.django_settings is not None:
        os.environ[ENVIRONMENT_VARIABLE] = opts.django_settings

    if not args and ENVIRONMENT_VARIABLE in os.environ:
        args = (DJANGO_HANDLER, )

    if not args:
        sys.exit('''Please specify the application to run: pass WSGI app spec as an argument.

If you're trying to run a Django app, setting DJANGO_SETTINGS_MODULE environment variable
or passing --django-settings options would also work.

Type %s -h for help.''' % sys.argv[0])

    if len(args)!=1:
        sys.exit('Too many arguments. Type %s -h for help.' % sys.argv[0])

    wsgi_app = load_app_spec(args[0])

    master = Master((opts.host, opts.port), wsgi_app)
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
    dctx.chroot_directory = opts.chroot
    # Better to explicitly state that no signal mapping should be done; we do
    # this with libevent and I'd think these interfere with eachother.
    # TODO Subclass DaemonContext and make the signal map use gevent?
    dctx.signal_map = {}
    if opts.pidfile:
        dctx.pidfile = PIDLockFile(opts.pidfile, threaded=False)
        print "pidfile =", dctx.pidfile
    if opts.user:
        from pwd import getpwnam
        dctx.uid = getpwnam(opts.user).pw_uid
    if opts.group:
        from grp import getgrnam
        dctx.gid = getgrnam(opts.group).gr_gid
    # Go!
    with dctx:
        gevent.reinit()  # Needs to be done as dctx might've forked.
        master.spawn_workers(opts.workers)
        try:
            master.serve_forever()
        except KeyboardInterrupt:
            master.stop()


if __name__ == "__main__":
    import sys
    main(sys.argv[1:], [sys.executable] + sys.argv[1:])
