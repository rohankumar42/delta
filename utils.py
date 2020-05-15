import time
from datetime import datetime
from queue import Queue


def avg(x):
    if isinstance(x, Queue):
        x = x.queue

    return sum(x) / len(x)


def fmt_time(t=None, fmt='%H:%M:%S'):
    return datetime.fromtimestamp(t or time.time()).strftime(fmt)
