import time
from datetime import datetime
from queue import Queue

try:
    from termcolor import colored
except ImportError:
    def colored(x, *args, **kwargs):
        return x


def avg(x):
    if isinstance(x, Queue):
        x = x.queue

    return sum(x) / len(x)


def fmt_time(t=None, fmt='%H:%M:%S'):
    return datetime.fromtimestamp(t or time.time()).strftime(fmt)
