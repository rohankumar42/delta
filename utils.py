import yaml
import time
from datetime import datetime
from queue import Queue

try:
    from termcolor import colored
except ImportError:
    def colored(x, *args, **kwargs):
        return x

with open('endpoints.yaml') as fh:
    ENDPOINTS = yaml.safe_load(fh)


def avg(x):
    if isinstance(x, Queue):
        x = x.queue

    return sum(x) / len(x)


def fmt_time(t=None, fmt='%H:%M:%S'):
    return datetime.fromtimestamp(t or time.time()).strftime(fmt)


def endpoint_name(endpoint):
    name = ENDPOINTS[endpoint]['name']
    return f'{name:20}'
