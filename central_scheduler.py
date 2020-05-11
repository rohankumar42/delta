import time
import logging
from collections import defaultdict
from queue import Queue

try:
    from termcolor import colored
except ImportError:
    def colored(x, *args, **kwargs):
        return x

from funcx import FuncXClient
from funcx.serialize import FuncXSerializer
from strategies import init_strategy


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter(
    colored("[SCHEDULER] %(message)s", 'yellow')))
logger.addHandler(ch)


class CentralScheduler(object):

    def __init__(self, fxc=None, endpoints=None, strategy='round-robin',
                 last_n_times=3, log_level='INFO', *args, **kwargs):
        self._fxc = fxc or FuncXClient(*args, **kwargs)

        # List of all FuncX endpoints we can execute on
        self._endpoints = list(set(endpoints or []))

        # Track which endpoints a function can't run on
        self._blacklists = defaultdict(set)

        # Average times for each function on each endpoint
        self._last_n_times = last_n_times
        self._runtimes = defaultdict(lambda: defaultdict(Queue))
        self._avg_runtimes = defaultdict(lambda: defaultdict(float))
        self._num_executions = defaultdict(lambda: defaultdict(int))

        # Track pending tasks
        self._pending = {}
        # TODO: backup tasks?

        # Set logging levels
        logger.setLevel(log_level)

        # Intialize serializer
        self.fx_serializer = FuncXSerializer()
        self.fx_serializer.use_custom('03\n', 'code')

        # Initialize scheduling strategy
        self.strategy = init_strategy(strategy, endpoints=self._endpoints,
                                      runtimes=self._avg_runtimes)
        logger.info(f"Scheduler using strategy {strategy}")

    def blacklist(self, func, endpoint):
        # TODO: use blacklists in scheduling
        if endpoint not in self._endpoints:
            logger.error('Cannot blacklist unknown endpoint {}'
                         .format(endpoint))
        else:
            logger.info('Blacklisting endpoint {} for function {}'
                        .format(endpoint, func))
            self._blacklists[func].add(endpoint)

        # TODO: return response message?

    def choose_endpoint(self, func):
        endpoint = self.strategy.choose_endpoint(func)
        logger.debug('Choosing endpoint {} for func {}'.format(endpoint, func))
        return endpoint

    def log_submission(self, func, endpoint, task_id):
        info = {
            'time_sent': time.time(),
            'function_id': func,
            'endpoint_id': endpoint,
        }

        logger.info('Sending func {} to endpoint {} with task id {}'
                    .format(func, endpoint, task_id))
        self._pending[task_id] = info

        return endpoint

    def log_status(self, task_id, data):
        if task_id not in self._pending:
            logger.warn('Ignoring unknown task id {}'.format(task_id))
            return

        if 'result' in data:
            result = self.fx_serializer.deserialize(data['result'])
            self._record_result(task_id, result)
            del self._pending[task_id]
        elif 'exception' in data:
            exception = self.fx_serializer.deserialize(data['exception'])
            self._record_exception(task_id, exception)
            del self._pending[task_id]
        elif 'status' in data and data['status'] == 'PENDING':
            pass
        else:
            logger.error('Unexpected status message: {}'.format(data))

    def _record_result(self, task_id, result):
        info = self._pending[task_id]
        logger.info('Got result from {} for task {} with time {}'
                    .format(info['endpoint_id'], task_id, result['runtime']))
        self._update_runtimes(task_id, result['runtime'])

    def _record_exception(self, task_id, exception):
        try:
            exception.reraise()
        except Exception as e:
            logger.error('Got exception on task {}: {}'
                         .format(task_id, e))

    def _update_runtimes(self, task_id, new_runtime):
        info = self._pending[task_id]
        func = info['function_id']
        end = info['endpoint_id']

        while len(self._runtimes[func][end].queue) > self._last_n_times:
            self._runtimes[func][end].get()
        self._runtimes[func][end].put(new_runtime)
        self._avg_runtimes[func][end] = avg(self._runtimes[func][end])

        self._num_executions[func][end] += 1


##############################################################################
#                           Utility Functions
##############################################################################

def avg(x):
    if isinstance(x, Queue):
        x = x.queue

    return sum(x) / len(x)
