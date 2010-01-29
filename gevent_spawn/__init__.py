"""Helper package for gevent-spawn"""

import signal

#signal_reload = signal.SIGHUP  # Unused
# Signal used to stop a worker
signal_stop = signal.SIGUSR1
