import time
import json
import uuid
import logging
import requests
from queue import Queue, Empty
from threading import Thread
from collections import defaultdict

from funcx import FuncXClient
from funcx.serialize import FuncXSerializer
from utils import colored
from strategies import init_strategy
from predictors import init_runtime_predictor


logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter(
    colored("[SCHEDULER] %(message)s", 'yellow')))
logger.addHandler(ch)


FUNCX_API = 'https://dev.funcx.org/api/v1'
HEARTBEAT_THRESHOLD = 75.0  # Endpoints send regular heartbeats


class CentralScheduler(object):

    def __init__(self, endpoints, strategy='round-robin',
                 runtime_predictor='rolling-average', last_n=3, train_every=1,
                 log_level='INFO', *args, **kwargs):
        self._fxc = FuncXClient(*args, **kwargs)

        # Info about FuncX endpoints we can execute on
        self._endpoints = endpoints
        self.is_dead = defaultdict(bool)
        self.temperature = defaultdict(lambda: 'WARM')

        # Track which endpoints a function can't run on
        self._blacklists = defaultdict(set)

        # Track pending tasks
        # We will provide the client our own task ids, since we may submit the
        # same task multiple times to the FuncX service, and sometimes we may
        # wait to submit a task to FuncX (e.g., wait for a data transfer).
        self._task_id_translation = {}
        self._pending = {}
        self._pending_by_endpoint = defaultdict(set)
        self._latest_status = {}
        self._last_task_ETA = {}
        # Estimated error in the pending-task time of an endpoint.
        # Updated every time a task result is received from an endpoint.
        self._queue_error = defaultdict(float)
        # self._num_backups_sent = {}  # TODO: backup tasks

        # Set logging levels
        logger.setLevel(log_level)

        # Intialize serializer
        self.fx_serializer = FuncXSerializer()
        self.fx_serializer.use_custom('03\n', 'code')

        # Initialize runtime predictor
        self.predictor = init_runtime_predictor(runtime_predictor,
                                                endpoints=endpoints,
                                                last_n=last_n,
                                                train_every=train_every)
        logger.info(f"Runtime predictor using strategy {self.predictor}")

        # Initialize scheduling strategy
        self.strategy = init_strategy(strategy, endpoints=endpoints,
                                      runtime_predictor=self.predictor,
                                      queue_predictor=self.queue_delay,
                                      launch_predictor=self.launch_time)
        logger.info(f"Scheduler using strategy {self.strategy}")

        # Start thread to check on endpoints regularly
        self._endpoint_watchdog = Thread(target=self._check_endpoints)
        self._endpoint_watchdog.start()

        # Start thread to monitor tasks and send tasks to FuncX service
        self._scheduled_tasks = Queue()
        self._task_watchdog_sleep = 0.15
        self._task_watchdog = Thread(target=self._monitor_tasks)
        self._task_watchdog.start()

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

    def batch_submit(self, tasks, headers):
        # TODO: smarter scheduling for batch submissions

        task_ids = []
        endpoints = []

        for func, payload in tasks:
            # TODO: do not choose a dead or blacklisted endpoint
            choice = self.strategy.choose_endpoint(func, payload)
            endpoint = choice['endpoint']
            logger.debug('Choosing endpoint {} for func {}'
                         .format(self.endpoint_name(endpoint), func))
            choice['ETA'] = choice.get('ETA', time.time())

            # If a cold endpoint is being started, mark it as no longer cold,
            # so that subsequent launch-time predictions are correct (i.e., 0)
            if self.temperature[endpoint] == 'COLD':
                self.temperature[endpoint] = 'WARMING'
                logger.info('A cold endpoint {} was chosen; marked as warming.'
                            .format(self.endpoint_name(endpoint)))

            # Store task information
            task_id = str(uuid.uuid4())
            self._task_id_translation[task_id] = set()
            info = {
                'task_id': task_id,
                'ETA': choice['ETA'],
                'function_id': func,
                'endpoint_id': endpoint,
                'payload': payload,
                'headers': headers
            }

            # Schedule task for sending
            self._scheduled_tasks.put((task_id, info))

            task_ids.append(task_id)
            endpoints.append(endpoint)

        return task_ids, endpoints

    def translate_task_id(self, task_id):
        return self._task_id_translation[task_id]

    def log_status(self, real_task_id, data):
        if real_task_id not in self._pending:
            logger.warn('Ignoring unknown task id {}'.format(real_task_id))
            return

        task_id = self._pending[real_task_id]['task_id']
        # Don't overwrite latest status if it is a result/exception
        if task_id not in self._latest_status or \
                self._latest_status[task_id].get('status') == 'PENDING':
            self._latest_status[task_id] = data

        if 'result' in data:
            result = self.fx_serializer.deserialize(data['result'])
            runtime = result['runtime']
            endpoint = self._pending[real_task_id]['endpoint_id']
            name = self.endpoint_name(endpoint)
            logger.info('Got result from {} for task {} with time {}'
                        .format(name, real_task_id, runtime))

            self.predictor.update(self._pending[real_task_id], runtime)
            self._record_completed(real_task_id)

        elif 'exception' in data:
            exception = self.fx_serializer.deserialize(data['exception'])
            try:
                exception.reraise()
            except Exception as e:
                logger.error('Got exception on task {}: {}'
                             .format(real_task_id, e))

            self._record_completed(real_task_id)

        elif 'status' in data and data['status'] == 'PENDING':
            pass

        else:
            logger.error('Unexpected status message: {}'.format(data))

    def get_status(self, task_id):
        if task_id not in self._task_id_translation:
            logger.warn('Unknown client task id {}'.format(task_id))

        elif len(self._task_id_translation[task_id]) == 0:
            return {'status': 'PENDING'}  # Task has not been scheduled yet

        elif task_id not in self._latest_status:
            return {'status': 'PENDING'}  # Status has not been queried yet

        else:
            return self._latest_status[task_id]

    def queue_delay(self, endpoint):
        # If there are no pending tasks on endpoint, no queue delay.
        # Otherwise, queue delay is the ETA of most recent task,
        # plus the estimated error in the ETA prediction.
        if len(self._pending_by_endpoint[endpoint]) == 0:
            delay = time.time()
        else:
            delay = self._last_task_ETA[endpoint] + self._queue_error[endpoint]
            delay = max(delay, time.time())

        return delay

    def _record_completed(self, real_task_id):
        info = self._pending[real_task_id]
        endpoint = info['endpoint_id']

        # If this is the last pending task on this endpoint, reset ETA offset
        if len(self._pending_by_endpoint[endpoint]) == 1:
            self._queue_error[endpoint] = 0.0
        else:
            prediction_error = time.time() - self._pending[real_task_id]['ETA']
            self._queue_error[endpoint] = prediction_error
            # print(colored(f'Prediction error {prediction_error}', 'red'))

        logger.info('Task exec time: expected = {:.3f}, actual = {:.3f}'
                    .format(info['ETA'] - info['time_sent'],
                            time.time() - info['time_sent']))
        # logger.info(f'ETA_offset = {self._queue_error[endpoint]:.3f}')

        # Stop tracking this task
        del self._pending[real_task_id]
        self._pending_by_endpoint[endpoint].remove(real_task_id)

    def endpoint_name(self, endpoint):
        name = self._endpoints[endpoint]['name']
        return f'{name:16}'

    def launch_time(self, endpoint):
        # If endpoint is warm, there is no launch time
        if self.temperature[endpoint] != 'COLD':
            return 0.0
        # Otherwise, return the launch time in the endpoint config
        elif 'launch_time' in self._endpoints[endpoint]:
            return self._endpoints[endpoint]['launch_time']
        else:
            logger.warn('Endpoint {} should always be warm, but is cold'
                        .format(self.endpoint_name(endpoint)))
            return 0.0

    def _monitor_tasks(self):
        logger.info('Starting task-watchdog thread')

        while True:

            time.sleep(self._task_watchdog_sleep)

            task_infos = {}
            while True:
                try:
                    task_id, info = self._scheduled_tasks.get_nowait()
                    task_infos[task_id] = info
                except Empty:
                    break

            if len(task_infos) == 0:
                logger.debug('No scheduled tasks. Task watchdog sleeping...')
                continue

            # TODO: different clients send different headers. change eventually
            headers = list(task_infos.values())[0]['headers']

            logger.info('Scheduling a batch of {} tasks'
                        .format(len(task_infos)))

            # Submit scheduled tasks to FuncX
            data = {'tasks': []}
            for task_id, info in task_infos.items():
                submit_info = (info['function_id'], info['endpoint_id'],
                               info['payload'])
                data['tasks'].append(submit_info)

            res_str = requests.post(f'{FUNCX_API}/submit', headers=headers,
                                    data=json.dumps(data))
            res = json.loads(res_str.text)
            if res['status'] != 'Success':
                logger.error('Could not send tasks to FuncX. Got response: {}'
                             .format(res))
                continue

            # Update task info with submission info
            for task_id, real_task_id in zip(task_infos.keys(),
                                             res['task_uuids']):
                info = task_infos[task_id]
                info['ETA'] = self.strategy.predict_ETA(info['function_id'],
                                                        info['endpoint_id'],
                                                        info['payload'])
                info['time_sent'] = time.time()

                endpoint = info['endpoint_id']
                self._task_id_translation[task_id].add(real_task_id)

                self._pending[real_task_id] = info
                self._pending_by_endpoint[endpoint].add(real_task_id)

                # Record endpoint ETA for queue-delay prediction
                self._last_task_ETA[endpoint] = info['ETA']

                logger.debug('Sent task id {} with real task id {}'
                             .format(task_id, real_task_id))

    def _check_endpoints(self):
        logger.info('Starting endpoint-watchdog thread')
        fxc = FuncXClient()

        while True:
            for end in self._endpoints.keys():
                statuses = fxc.get_endpoint_status(end)
                if len(statuses) == 0:
                    logger.warn('Endpoint {} does not have any statuses'
                                .format(self.endpoint_name(end)))
                else:
                    status = statuses[0]  # Most recent endpoint status

                    # Mark endpoint as dead/alive based on heartbeat's age
                    age = time.time() - status['timestamp']
                    if not self.is_dead[end] and age > HEARTBEAT_THRESHOLD:
                        self.is_dead[end] = True
                        logger.warn('Endpoint {} seems to have died! '
                                    'Last heartbeat was {:.2f} seconds ago.'
                                    .format(self.endpoint_name(end), age))
                    elif self.is_dead[end] and age <= HEARTBEAT_THRESHOLD:
                        self.is_dead[end] = False
                        logger.warn('Endpoint {} is back alive! '
                                    'Last heartbeat was {:.2f} seconds ago.'
                                    .format(self.endpoint_name(end), age))

                    # Mark endpoint as "cold" or "warm" depending on if it
                    # has active managers (nodes) allocated to it
                    if self.temperature[end] == 'WARM' \
                            and status['active_managers'] == 0:
                        self.temperature[end] = 'COLD'
                        logger.info('Endpoint {} is cold!'
                                    .format(self.endpoint_name(end)))
                    elif self.temperature[end] != 'WARM' \
                            and status['active_managers'] > 0:
                        self.temperature[end] = 'WARM'
                        logger.info('Endpoint {} is warm again!'
                                    .format(self.endpoint_name(end)))

            # Sleep before checking statuses again
            time.sleep(15)
