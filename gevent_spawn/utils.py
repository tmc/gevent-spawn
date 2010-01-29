# {{{ setproctitle support
def setproctitle_noop(name):
    pass
try:
    from setproctitle import setproctitle as setproctitle_real
    _has_mod = True
except ImportError:
    _has_mod = False
setproctitle = setproctitle_real if _has_mod else setproctitle_noop
# }}}

# {{{ Importing a filename
import imp
def import_filename(filename):
    for suffix, mode, type in imp.get_suffixes():
        if filename.endswith(suffix):
            name = filename[:-len(suffix)]
            return imp.load_module(name, open(filename), filename, (suffix, mode, type))
    raise ImportError("couldn't find a loader for %r" % (filename,))
# }}}

# {{{ Signal name look-ups
import signal
# An ugly way to do this, but it works well.
signame_map = dict((v, a) for (a, v) in vars(signal).items()
                   if a.startswith("SIG"))
def signum2name(signum):
    return signame_map.get(signum, "SIG#" + str(signum))
# }}}
