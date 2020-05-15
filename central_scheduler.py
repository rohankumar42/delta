import time
from datetime import datetime
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
    FUNCX_LATENCY = 0.5  # Estimated overhead of executing task

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
        self._avg_runtime = defaultdict(lambda: defaultdict(float))
        self._num_executions = defaultdict(lambda: defaultdict(int))

        # Track pending tasks
        self._pending = {}
        self._pending_by_endpoint = defaultdict(set)
        self._last_task_sent = {}
        # Estimated error in the pending-task time of an endpoint.
        # Updated every time a task result is received from an endpoint.
        self._queue_error = defaultdict(float)
        # TODO: backup tasks?

        # Set logging levels
        logger.setLevel(log_level)

        # Intialize serializer
        self.fx_serializer = FuncXSerializer()
        self.fx_serializer.use_custom('03\n', 'code')

        # Initialize scheduling strategy
        self.strategy = init_strategy(strategy, endpoints=self._endpoints,
                                      runtimes=self._avg_runtime,
                                      ETA_predictor=self._predict_ETA)
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
        choice = self.strategy.choose_endpoint(func)
        logger.debug('Choosing endpoint {} for func {}'
                     .format(choice['endpoint'], func))
        return choice

    def log_submission(self, func, choice, task_id):
        endpoint = choice['endpoint']
        expected_ETA = choice.get('ETA', time.time())

        logger.info('Sending func {} to endpoint {} with task id {}'
                    .format(func, endpoint, task_id))
        # logger.info('Task ETA is {:.2f} s from now'
        # .format(expected_ETA - time.time()))

        info = {
            'time_sent': time.time(),
            'ETA': expected_ETA,
            'function_id': func,
            'endpoint_id': endpoint,
        }
        self._pending[task_id] = info
        self._pending_by_endpoint[endpoint].add(task_id)
        self._last_task_sent[endpoint] = task_id

        return endpoint

    def log_status(self, task_id, data):
        if task_id not in self._pending:
            logger.warn('Ignoring unknown task id {}'.format(task_id))
            return

        if 'result' in data:
            result = self.fx_serializer.deserialize(data['result'])

            logger.info('Got result from {} for task {} with time {}'
                        .format(self._pending[task_id]['endpoint_id'],
                                task_id, result['runtime']))

            self._update_runtimes(task_id, result['runtime'])
            self._record_completed(task_id)

        elif 'exception' in data:
            exception = self.fx_serializer.deserialize(data['exception'])
            try:
                exception.reraise()
            except Exception as e:
                logger.error('Got exception on task {}: {}'
                             .format(task_id, e))

            self._record_completed(task_id)

        elif 'status' in data and data['status'] == 'PENDING':
            pass

        else:
            logger.error('Unexpected status message: {}'.format(data))

    def _predict_ETA(self, func, endpoint):
        # TODO: use function input for prediction
        # TODO: better task ETA prediction by including data movement,
        # latency, start-up, and other costs

        t_pending = self._queue_delay(endpoint)
        t_run = self._avg_runtime[func][endpoint]

        # logger.info('[{} s], QD = {} s, RT = {:.2f} s, ETA = {} s'
        # .format(fmt_time(), fmt_time(t_pending), t_run,
        # fmt_time(t_pending + t_run)))
        return t_pending + t_run + self.FUNCX_LATENCY

    def _queue_delay(self, endpoint):
        # If there are no pending tasks on endpoint, no queue delay.
        # Otherwise, queue delay is the ETA of most recent task,
        # plus the estimated error in the ETA prediction.
        if endpoint not in self._last_task_sent or \
                self._last_task_sent[endpoint] not in self._pending:
            return time.time()
        else:
            last = self._last_task_sent[endpoint]
            return self._pending[last]['ETA'] + self._queue_error[endpoint]

    def _record_completed(self, task_id):
        info = self._pending[task_id]
        endpoint = info['endpoint_id']

        if self._last_task_sent.get(endpoint) == task_id:
            del self._last_task_sent[endpoint]

        # If this is the last pending task on this endpoint, reset ETA offset
        if len(self._pending_by_endpoint[endpoint]) == 1:
            self._queue_error[endpoint] = 0.0
        else:
            prediction_error = time.time() - self._pending[task_id]['ETA']
            self._queue_error[endpoint] = prediction_error

        logger.info('Task exec time: expected = {:.3f}, actual = {:.3f}'
                    .format(info['ETA'] - info['time_sent'],
                            time.time() - info['time_sent']))
        # logger.info(f'ETA_offset = {self._queue_error[endpoint]:.3f}')

        del self._pending[task_id]
        self._pending_by_endpoint[endpoint].remove(task_id)

    def _update_runtimes(self, task_id, new_runtime):
        info = self._pending[task_id]
        func = info['function_id']
        end = info['endpoint_id']

        while len(self._runtimes[func][end].queue) > self._last_n_times:
            self._runtimes[func][end].get()
        self._runtimes[func][end].put(new_runtime)
        self._avg_runtime[func][end] = avg(self._runtimes[func][end])

        self._num_executions[func][end] += 1


##############################################################################
#                           Utility Functions
##############################################################################

def avg(x):
    if isinstance(x, Queue):
        x = x.queue

    return sum(x) / len(x)


def fmt_time(t=None, fmt='%H:%M:%S'):
    return datetime.fromtimestamp(t or time.time()).strftime(fmt)
